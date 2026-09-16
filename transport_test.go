package bugfree

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestHTTPTransportSendsEvent(t *testing.T) {
	received := make(chan Event, 1)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, expected POST", r.Method)
		}
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Errorf("content-type = %q", got)
		}
		if got := r.Header.Get("User-Agent"); got != "bugfree-go/"+Version {
			t.Errorf("user-agent = %q", got)
		}
		if r.URL.Path != "/ingest/v1/key123/store" {
			t.Errorf("path = %q", r.URL.Path)
		}

		var event Event
		_ = json.NewDecoder(r.Body).Decode(&event)
		received <- event
		_ = json.NewEncoder(w).Encode(ingestResponse{ShortID: "BF-1", IsNew: true})
	}))
	defer server.Close()

	client, err := NewClient(Options{
		DSN:         "http://key123@" + server.Listener.Addr().String() + "/ingest",
		Environment: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	client.CaptureMessage(LevelError, "over the wire")
	if !client.Flush(2 * time.Second) {
		t.Fatal("Flush timed out")
	}

	select {
	case event := <-received:
		if event.Message != "over the wire" {
			t.Errorf("message = %q", event.Message)
		}
		if event.Platform != "go" {
			t.Errorf("platform = %q", event.Platform)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no event reached the server")
	}
}

// A 4xx from the server must not be retried (an invalid key and the like).
func TestHTTPTransportDoesNotRetryClientError(t *testing.T) {
	var calls int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"code":"unauthorized"}`))
	}))
	defer server.Close()

	transport := newHTTPTransport(server.URL, nil, false)
	defer transport.Close()

	transport.Send(&Event{Level: LevelError, Type: "X", Message: "m"})
	transport.Flush(2 * time.Second)
	time.Sleep(200 * time.Millisecond)

	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("request count = %d, expected 1 (no retry on 4xx)", got)
	}
}

// A server error (5xx) has to be retried once.
func TestHTTPTransportRetriesServerError(t *testing.T) {
	var calls int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	transport := newHTTPTransport(server.URL, nil, false)
	defer transport.Close()

	transport.Send(&Event{Level: LevelError, Type: "X", Message: "m"})
	time.Sleep(1200 * time.Millisecond)

	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Errorf("request count = %d, expected 2 (one retry)", got)
	}
}

// An unreachable server must not block the application.
func TestHTTPTransportDoesNotBlockOnUnreachableServer(t *testing.T) {
	transport := newHTTPTransport("http://127.0.0.1:1/store", &http.Client{Timeout: 100 * time.Millisecond}, false)
	defer transport.Close()

	done := make(chan struct{})
	go func() {
		for i := 0; i < 10; i++ {
			transport.Send(&Event{Level: LevelError, Type: "X", Message: "m"})
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Send blocked")
	}
}

// When the queue fills up, events are dropped but Send does not block.
func TestHTTPTransportDropsWhenQueueIsFull(t *testing.T) {
	// A slow but finite server: the queue fills while the worker is busy with one event.
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		time.Sleep(20 * time.Millisecond)
	}))
	defer server.Close()

	transport := newHTTPTransport(server.URL, &http.Client{Timeout: time.Second}, false)
	defer transport.Close()

	started := time.Now()
	for i := 0; i < queueSize*3; i++ {
		transport.Send(&Event{Level: LevelError, Type: "X", Message: "m"})
	}
	// The queue is bounded, so do not expect as many calls as there were sends.
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Errorf("Send took %v; the queue is blocking", elapsed)
	}
	// What was dropped is counted, so Client.Stats can report it.
	if transport.droppedFull.Load() == 0 {
		t.Error("the dropped events were not counted")
	}
}

// Flush has to watch the in-flight event, not only the queue.
func TestFlushWaitsForInFlightEvent(t *testing.T) {
	delivered := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(150 * time.Millisecond)
		delivered <- struct{}{}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	transport := newHTTPTransport(server.URL, nil, false)
	defer transport.Close()

	transport.Send(&Event{Level: LevelError, Type: "X", Message: "m"})

	if !transport.Flush(3 * time.Second) {
		t.Fatal("Flush timed out")
	}
	select {
	case <-delivered:
	default:
		t.Error("Flush returned before the event was delivered")
	}
}

// Close must not wait for every queued event against a server that never
// answers; it gives up after closeTimeout and aborts the request in flight.
func TestHTTPTransportCloseIsBounded(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(10 * time.Second):
		}
	}))
	defer server.Close()

	transport := newHTTPTransport(server.URL, &http.Client{Timeout: 5 * time.Second}, false)
	transport.closeTimeout = 200 * time.Millisecond

	for i := 0; i < 20; i++ {
		transport.Send(&Event{Level: LevelError, Type: "X", Message: "m"})
	}

	started := time.Now()
	transport.Close()
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("Close took %v; it has to give up after the close timeout", elapsed)
	}
	if pending := transport.pending.Load(); pending != 0 {
		t.Errorf("pending = %d after Close, expected 0", pending)
	}
}

// Close called twice must not panic (a closed channel).
func TestHTTPTransportCloseIsIdempotent(t *testing.T) {
	transport := newHTTPTransport("http://127.0.0.1:1/store", nil, false)
	transport.Close()
	transport.Close()
	// After closing, sending is silently ignored.
	transport.Send(&Event{Level: LevelError, Type: "X", Message: "m"})
}

// A 429 pauses sending for as long as the server asked; nothing is retried or
// sent in the meantime.
func TestHTTPTransportPausesOnRateLimit(t *testing.T) {
	var calls int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.Header().Set("Retry-After", "30")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()

	transport := newHTTPTransport(server.URL, nil, false)
	defer transport.Close()

	transport.Send(&Event{Level: LevelError, Type: "X", Message: "first"})
	transport.Flush(2 * time.Second)

	for range 5 {
		transport.Send(&Event{Level: LevelError, Type: "X", Message: "during the pause"})
	}
	transport.Flush(2 * time.Second)

	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("request count = %d, expected 1 (no sending while paused)", got)
	}
	if remaining := time.Until(time.Unix(0, transport.pausedUntil.Load())); remaining < 25*time.Second {
		t.Errorf("pause = %s, expected about the 30s the server asked for", remaining)
	}
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	cases := []struct {
		value string
		want  time.Duration
	}{
		{"120", 2 * time.Minute},
		{now.Add(90 * time.Second).Format(http.TimeFormat), 90 * time.Second},
		{"", defaultRetryAfter},
		{"-5", defaultRetryAfter},
		{"soon", defaultRetryAfter},
		{now.Add(-time.Hour).Format(http.TimeFormat), defaultRetryAfter},
	}
	for _, tc := range cases {
		if got := parseRetryAfter(tc.value, now); got != tc.want {
			t.Errorf("parseRetryAfter(%q) = %s, expected %s", tc.value, got, tc.want)
		}
	}
}
