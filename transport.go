package bugfree

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// Transport delivers events to the server. Replaceable in tests.
type Transport interface {
	Send(event *Event)
	Flush(timeout time.Duration) bool
	Close()
}

// httpTransport queues the events and sends them in the background.
//
// The queue is a bounded, non-blocking channel: monitoring must not add latency
// to the application it watches. When the queue is full, the event is dropped.
type httpTransport struct {
	url    string
	client *http.Client
	debug  bool

	queue chan *Event
	wg    sync.WaitGroup

	// pending is the number of events queued plus the ones being sent.
	// Flush has to watch the in-flight ones as well as the queue; otherwise the
	// last event is lost during shutdown.
	pending atomic.Int64

	// closeMu separates closing from sending. A lock-free "closed" check races,
	// and writing to a closed channel panics.
	closeMu sync.RWMutex
	closed  bool

	// closeTimeout is how long Close waits for the queue to drain. When it runs
	// out, ctx is cancelled: the request in flight is aborted and the events still
	// queued are dropped.
	closeTimeout time.Duration
	ctx          context.Context
	cancel       context.CancelFunc
}

// queueSize is how many events may wait in the queue.
const queueSize = 100

// defaultCloseTimeout bounds Close. Every queued event could otherwise cost two
// full request timeouts against an unreachable server, which adds up to minutes.
const defaultCloseTimeout = 5 * time.Second

// newHTTPTransport builds the transport and starts the worker goroutine.
func newHTTPTransport(url string, client *http.Client, debug bool) *httpTransport {
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}

	ctx, cancel := context.WithCancel(context.Background())
	transport := &httpTransport{
		url:          url,
		client:       client,
		debug:        debug,
		queue:        make(chan *Event, queueSize),
		closeTimeout: defaultCloseTimeout,
		ctx:          ctx,
		cancel:       cancel,
	}

	transport.wg.Add(1)
	go transport.worker()
	return transport
}

// Send queues the event; when the queue is full it is dropped silently.
func (t *httpTransport) Send(event *Event) {
	t.closeMu.RLock()
	defer t.closeMu.RUnlock()

	if t.closed {
		return
	}

	select {
	case t.queue <- event:
		t.pending.Add(1)
	default:
		t.logf("bugfree: queue is full, event dropped (%s)", event.Type)
	}
}

// worker sends the queued events one after another.
func (t *httpTransport) worker() {
	defer t.wg.Done()
	for event := range t.queue {
		// After Close gave up, the remaining events are drained without sending.
		if t.ctx.Err() == nil {
			t.post(event)
		}
		t.pending.Add(-1)
	}
}

// post sends a single event and retries once on a transient failure.
func (t *httpTransport) post(event *Event) {
	body, err := json.Marshal(event)
	if err != nil {
		t.logf("bugfree: event could not be encoded: %v", err)
		return
	}

	for attempt := 0; attempt < 2; attempt++ {
		status, response, err := t.do(body)
		if err == nil && status < 300 {
			t.logf("bugfree: event stored (%s)", response.ShortID)
			return
		}

		// Client errors (an invalid key, a malformed body) are not retried.
		if status >= 400 && status < 500 {
			t.logf("bugfree: server rejected the event (%d)", status)
			return
		}
		if attempt == 0 {
			select {
			case <-time.After(500 * time.Millisecond):
				continue
			case <-t.ctx.Done():
				return
			}
		}
		t.logf("bugfree: event could not be sent: %v (status %d)", err, status)
	}
}

// do makes a single HTTP request.
func (t *httpTransport) do(body []byte) (int, ingestResponse, error) {
	timeout := t.client.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	ctx, cancel := context.WithTimeout(t.ctx, timeout)
	defer cancel()

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, t.url, bytes.NewReader(body))
	if err != nil {
		return 0, ingestResponse{}, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("User-Agent", "bugfree-go/"+Version)

	response, err := t.client.Do(request)
	if err != nil {
		return 0, ingestResponse{}, err
	}
	defer response.Body.Close()

	payload, _ := io.ReadAll(io.LimitReader(response.Body, 8*1024))
	if response.StatusCode >= 300 {
		return response.StatusCode, ingestResponse{}, fmt.Errorf("unexpected status: %s", string(payload))
	}

	var decoded ingestResponse
	_ = json.Unmarshal(payload, &decoded)
	return response.StatusCode, decoded, nil
}

// Flush waits for the pending and in-flight events to finish.
//
// It returns false when the timeout passes; the caller may continue shutting down.
func (t *httpTransport) Flush(timeout time.Duration) bool {
	deadline := time.After(timeout)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		if t.pending.Load() == 0 {
			return true
		}
		select {
		case <-deadline:
			return false
		case <-ticker.C:
		}
	}
}

// Close stops accepting events and waits for the queue to drain, at most
// closeTimeout. What is left after that is dropped.
func (t *httpTransport) Close() {
	t.closeMu.Lock()
	if t.closed {
		t.closeMu.Unlock()
		return
	}
	t.closed = true
	close(t.queue)
	t.closeMu.Unlock()

	done := make(chan struct{})
	go func() {
		t.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(t.closeTimeout):
		t.logf("bugfree: close timed out, %d event(s) dropped", t.pending.Load())
		// Aborts the request in flight; the worker then skips the rest and exits.
		t.cancel()
		<-done
	}
	t.cancel()
}

// logf only writes when Debug is on.
func (t *httpTransport) logf(format string, args ...any) {
	if t.debug {
		log.Printf(format, args...)
	}
}
