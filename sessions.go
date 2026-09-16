package bugfree

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"time"
)

// SessionStatus is how a request session ended.
type SessionStatus string

// The outcomes a request session is counted under.
const (
	// SessionOK ended normally.
	SessionOK SessionStatus = "ok"
	// SessionErrored answered with a server error.
	SessionErrored SessionStatus = "errored"
	// SessionCrashed ended in a panic.
	SessionCrashed SessionStatus = "crashed"
)

// SessionCounts is one minute of request sessions, as they are reported.
type SessionCounts struct {
	Release     string `json:"release"`
	Environment string `json:"environment"`
	Started     string `json:"started"`
	Total       int64  `json:"total"`
	Errored     int64  `json:"errored"`
	Crashed     int64  `json:"crashed"`
}

// SessionSender is a Transport that also delivers session counts, as the SDK's
// own test transports do.
type SessionSender interface {
	SendSessions(counts []SessionCounts)
}

// sessionFlushInterval is how often the counted sessions are reported.
const sessionFlushInterval = time.Minute

// sessionAggregator counts request sessions by minute and reports them in the
// background. A server has far too many requests for one report each.
type sessionAggregator struct {
	client *Client

	mu      sync.Mutex
	minutes map[time.Time]*SessionCounts

	stop chan struct{}
	done chan struct{}
}

func newSessionAggregator(client *Client) *sessionAggregator {
	aggregator := &sessionAggregator{
		client:  client,
		minutes: map[time.Time]*SessionCounts{},
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
	go aggregator.run()
	return aggregator
}

func (a *sessionAggregator) record(status SessionStatus, at time.Time) {
	minute := at.UTC().Truncate(time.Minute)
	a.mu.Lock()
	defer a.mu.Unlock()

	counts := a.minutes[minute]
	if counts == nil {
		counts = &SessionCounts{
			Release:     a.client.options.Release,
			Environment: a.client.options.Environment,
			Started:     timestamp(minute),
		}
		a.minutes[minute] = counts
	}
	counts.Total++
	switch status {
	case SessionErrored:
		counts.Errored++
	case SessionCrashed:
		counts.Errored++
		counts.Crashed++
	}
}

// take removes and returns everything counted so far.
func (a *sessionAggregator) take() []SessionCounts {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.minutes) == 0 {
		return nil
	}
	counts := make([]SessionCounts, 0, len(a.minutes))
	for _, minute := range a.minutes {
		counts = append(counts, *minute)
	}
	a.minutes = map[time.Time]*SessionCounts{}
	return counts
}

func (a *sessionAggregator) run() {
	defer close(a.done)
	ticker := time.NewTicker(sessionFlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			a.flush()
		case <-a.stop:
			a.flush()
			return
		}
	}
}

// close reports what is left and stops the background loop.
func (a *sessionAggregator) close() {
	select {
	case <-a.stop:
	default:
		close(a.stop)
	}
	<-a.done
}

func (a *sessionAggregator) flush() {
	counts := a.take()
	if len(counts) == 0 {
		return
	}
	if sender, ok := a.client.transport.(SessionSender); ok {
		sender.SendSessions(counts)
		return
	}
	if err := a.client.postSessions(counts); err != nil && a.client.options.Debug {
		log.Printf("bugfree: sessions could not be reported: %v", err)
	}
}

func (c *Client) postSessions(counts []SessionCounts) error {
	body, err := json.Marshal(map[string]any{"sessions": counts})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), checkInTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.dsn.base+"/sessions", bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("User-Agent", "bugfree-go/"+Version)

	client := c.options.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: checkInTimeout}
	}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		return fmt.Errorf("status %d", response.StatusCode)
	}
	return nil
}

// RecordSession counts one request session for release health: a request that
// ended normally, answered with a server error, or panicked. The middlewares call
// it when TrackSessions is on; call it yourself for other frameworks.
func (c *Client) RecordSession(status SessionStatus) {
	if !c.Enabled() || c.sessions == nil {
		return
	}
	c.sessions.record(status, time.Now())
}

// TracksSessions reports whether request sessions are counted.
func (c *Client) TracksSessions() bool {
	return c.Enabled() && c.sessions != nil
}

// sessionForStatus maps an HTTP status onto a session outcome.
func sessionForStatus(status int) SessionStatus {
	if status >= http.StatusInternalServerError {
		return SessionErrored
	}
	return SessionOK
}

// statusWriter remembers the status a handler answered with. It keeps the
// optional interfaces of the writer it wraps reachable: Flush and Hijack directly,
// the rest through Unwrap and http.ResponseController.
type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusWriter) Write(body []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(body)
}

func (w *statusWriter) Flush() {
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (w *statusWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if hijacker, ok := w.ResponseWriter.(http.Hijacker); ok {
		return hijacker.Hijack()
	}
	return nil, nil, http.ErrNotSupported
}

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
