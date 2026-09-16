package bugfree

import (
	"bufio"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

// sessionTransport records events and session reports.
type sessionTransport struct {
	recordingTransport
	mu      sync.Mutex
	reports [][]SessionCounts
}

func (t *sessionTransport) SendSessions(counts []SessionCounts) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.reports = append(t.reports, counts)
}

func TestMiddlewareCountsRequestSessions(t *testing.T) {
	transport := &sessionTransport{}
	client, err := NewClient(Options{DSN: "http://key@localhost/ingest", Release: "api@1.2.0", Environment: "production", Transport: transport, TrackSessions: true})
	if err != nil {
		t.Fatal(err)
	}
	installForTest(t, client)

	handler := Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/fail":
			w.WriteHeader(http.StatusBadGateway)
		case "/panic":
			panic("boom")
		default:
			_, _ = w.Write([]byte("ok"))
		}
	}))
	for _, path := range []string{"/", "/", "/fail", "/panic"} {
		func() {
			defer func() { _ = recover() }()
			handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, path, nil))
		}()
	}
	client.Close()

	var total, errored, crashed int64
	for _, report := range transport.reports {
		for _, counts := range report {
			if counts.Release != "api@1.2.0" || counts.Environment != "production" {
				t.Errorf("counts = %+v", counts)
			}
			total, errored, crashed = total+counts.Total, errored+counts.Errored, crashed+counts.Crashed
		}
	}
	if total != 4 || errored != 2 || crashed != 1 {
		t.Errorf("total = %d, errored = %d, crashed = %d; expected 4, 2 and 1", total, errored, crashed)
	}
}

func TestSessionsAreOffByDefault(t *testing.T) {
	client, _ := newTestClient(t, nil)
	if client.TracksSessions() {
		t.Error("sessions are opt-in on a server")
	}
	client.RecordSession(SessionCrashed)
}

// hijackable is a writer that supports Hijack.
type hijackable struct {
	*httptest.ResponseRecorder
	hijacked bool
}

func (h *hijackable) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h.hijacked = true
	return nil, nil, nil
}

func TestStatusWriterKeepsOptionalInterfaces(t *testing.T) {
	inner := &hijackable{ResponseRecorder: httptest.NewRecorder()}
	writer := &statusWriter{ResponseWriter: inner}

	if _, _, err := http.NewResponseController(writer).Hijack(); err != nil || !inner.hijacked {
		t.Errorf("hijack err = %v, hijacked = %v", err, inner.hijacked)
	}
	writer.WriteHeader(http.StatusTeapot)
	writer.WriteHeader(http.StatusOK)
	if writer.status != http.StatusTeapot {
		t.Errorf("status = %d, the first status counts", writer.status)
	}
}
