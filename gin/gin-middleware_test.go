package bugfreegin

import (
	"bytes"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	bugfree "github.com/subnomic/bugfree-go-sdk"
)

// recordingTransport keeps the sent events in memory.
type recordingTransport struct {
	mu     sync.Mutex
	events []*bugfree.Event
}

func (t *recordingTransport) Send(event *bugfree.Event) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.events = append(t.events, event)
}

func (t *recordingTransport) Flush(time.Duration) bool { return true }
func (t *recordingTransport) Close()                   {}

func (t *recordingTransport) all() []*bugfree.Event {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]*bugfree.Event(nil), t.events...)
}

func newRouter(t *testing.T, options Options) (*gin.Engine, *recordingTransport) {
	t.Helper()
	gin.SetMode(gin.TestMode)

	transport := &recordingTransport{}
	if err := bugfree.Init(bugfree.Options{
		DSN:       "http://key@localhost:3000/ingest",
		Transport: transport,
	}); err != nil {
		t.Fatalf("init: %v", err)
	}
	t.Cleanup(bugfree.Close)

	router := gin.New()
	router.Use(Recovery(options))
	return router, transport
}

func serve(router *gin.Engine, path string) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
	return recorder
}

func TestRecoveryReportsPanicWithRequestScope(t *testing.T) {
	router, transport := newRouter(t, Options{})
	router.GET("/orders/:id", func(c *gin.Context) {
		Scope(c).SetUser(bugfree.User{ID: "alice"})
		panic("nil order")
	})

	if recorder := serve(router, "/orders/7"); recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, expected 500", recorder.Code)
	}

	events := transport.all()
	if len(events) != 1 {
		t.Fatalf("got %d events, expected 1", len(events))
	}
	event := events[0]
	if event.User == nil || event.User.ID != "alice" {
		t.Errorf("user = %+v, expected the one set on the request scope", event.User)
	}
	if event.Tags["route"] != "/orders/:id" {
		t.Errorf("route tag = %v", event.Tags["route"])
	}

	// The user set inside the request stays inside it.
	bugfree.CaptureException(errors.New("background"))
	if last := transport.all()[1]; last.User != nil {
		t.Errorf("user = %+v leaked to the global scope", last.User)
	}
}

func TestCaptureErrorsReportsAttachedErrorsOnServerError(t *testing.T) {
	router, transport := newRouter(t, Options{CaptureErrors: true})
	router.GET("/settings", func(c *gin.Context) {
		_ = c.Error(errors.New("settings could not be saved"))
		c.AbortWithStatus(http.StatusInternalServerError)
	})

	serve(router, "/settings")

	events := transport.all()
	if len(events) != 1 {
		t.Fatalf("got %d events, expected 1", len(events))
	}
	event := events[0]
	if event.Message != "settings could not be saved" || event.Level != bugfree.LevelError {
		t.Errorf("event = %s %q", event.Level, event.Message)
	}
	if event.Request == nil || event.Request.StatusCode != http.StatusInternalServerError {
		t.Errorf("request = %+v, expected the 500 status", event.Request)
	}
	if event.Culprit != "GET /settings" || len(event.Stacktrace) != 0 {
		t.Errorf("culprit = %q with %d frames, expected the route and no middleware stack", event.Culprit, len(event.Stacktrace))
	}
}

func TestCaptureErrorsIgnoresClientErrorsAndStaysOffByDefault(t *testing.T) {
	router, transport := newRouter(t, Options{CaptureErrors: true})
	router.GET("/bad", func(c *gin.Context) {
		_ = c.Error(errors.New("invalid body"))
		c.AbortWithStatus(http.StatusBadRequest)
	})
	serve(router, "/bad")
	if got := len(transport.all()); got != 0 {
		t.Errorf("got %d events for a 4xx, expected none", got)
	}

	router, transport = newRouter(t, Options{})
	router.GET("/fail", func(c *gin.Context) {
		_ = c.Error(errors.New("failed"))
		c.AbortWithStatus(http.StatusInternalServerError)
	})
	serve(router, "/fail")
	if got := len(transport.all()); got != 0 {
		t.Errorf("got %d events with CaptureErrors off, expected none", got)
	}
}

func TestRecoveryLogsASwallowedPanicEvenWithoutDSN(t *testing.T) {
	gin.SetMode(gin.TestMode)
	if err := bugfree.Init(bugfree.Options{}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(bugfree.Close)

	var logged bytes.Buffer
	router := gin.New()
	router.Use(Recovery(Options{Logger: log.New(&logged, "", 0)}))
	router.GET("/boom", func(*gin.Context) { panic("lost without a log") })

	if recorder := serve(router, "/boom"); recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d", recorder.Code)
	}
	if !strings.Contains(logged.String(), "panic recovered: lost without a log") || !strings.Contains(logged.String(), "goroutine") {
		t.Errorf("log = %q, expected the panic and its stack", logged.String())
	}
}

func TestRecoveryLetsAbortHandlerThrough(t *testing.T) {
	router, transport := newRouter(t, Options{Logger: log.New(io.Discard, "", 0)})
	router.GET("/abort", func(*gin.Context) { panic(http.ErrAbortHandler) })

	defer func() {
		if recovered := recover(); recovered != http.ErrAbortHandler {
			t.Errorf("recovered = %v, expected http.ErrAbortHandler to be re-raised", recovered)
		}
		if got := len(transport.all()); got != 0 {
			t.Errorf("got %d events, an abort is not an error", got)
		}
	}()
	serve(router, "/abort")
}

func TestRequestAddressLeavesTheQueryOutUnlessAskedFor(t *testing.T) {
	router, transport := newRouter(t, Options{Logger: log.New(io.Discard, "", 0)})
	router.GET("/reset", func(*gin.Context) { panic("boom") })
	serve(router, "/reset?token=secret")
	if url := transport.all()[0].Request.URL; url != "/reset" {
		t.Errorf("url = %q, expected no query string", url)
	}

	router, transport = newRouter(t, Options{SendQueryString: true, Logger: log.New(io.Discard, "", 0)})
	router.GET("/search", func(*gin.Context) { panic("boom") })
	serve(router, "/search?q=shoes")
	if url := transport.all()[0].Request.URL; url != "/search?q=shoes" {
		t.Errorf("url = %q, expected the query string kept", url)
	}
}

func TestResolversDecideRequestIDAndClientIP(t *testing.T) {
	router, transport := newRouter(t, Options{
		Logger:            log.New(io.Discard, "", 0),
		RequestIDResolver: func(c *gin.Context) string { return c.GetString("trace") },
		UserResolver: func(*gin.Context) bugfree.User {
			return bugfree.User{ID: "alice", IPAddress: "203.0.113.9"}
		},
	})
	router.GET("/x", func(c *gin.Context) {
		c.Set("trace", "trace-123")
		panic("boom")
	})
	serve(router, "/x")

	event := transport.all()[0]
	if event.TraceID != "trace-123" {
		t.Errorf("trace id = %q", event.TraceID)
	}
	if event.User.IPAddress != "203.0.113.9" {
		t.Errorf("ip = %q, expected the resolver's own address kept", event.User.IPAddress)
	}

	router, transport = newRouter(t, Options{
		Logger:       log.New(io.Discard, "", 0),
		OmitClientIP: true,
		UserResolver: func(*gin.Context) bugfree.User { return bugfree.User{ID: "bob"} },
	})
	router.GET("/y", func(*gin.Context) { panic("boom") })
	serve(router, "/y")
	if ip := transport.all()[0].User.IPAddress; ip != "" {
		t.Errorf("ip = %q, expected none", ip)
	}
}

// saveSettings plays a service function that returns an error with its stack.
func saveSettings() error {
	return bugfree.WithStack(errors.New("settings row is locked"))
}

func TestCaptureErrorsUsesTheStackOfAStackCarryingError(t *testing.T) {
	router, transport := newRouter(t, Options{CaptureErrors: true})
	router.Use(Breadcrumbs())
	router.GET("/settings", func(c *gin.Context) {
		_ = c.Error(saveSettings())
		c.AbortWithStatus(http.StatusInternalServerError)
	})

	serve(router, "/settings")

	event := transport.all()[0]
	if len(event.Stacktrace) == 0 || !strings.HasSuffix(event.Stacktrace[0].Function, ".saveSettings") {
		t.Errorf("stack = %+v, expected it to start where the error was created", event.Stacktrace)
	}
	if event.Type != "*errors.errorString" {
		t.Errorf("type = %q", event.Type)
	}
	// Breadcrumbs registered after Recovery records on the request's scope.
	if len(event.Breadcrumbs) != 1 || event.Breadcrumbs[0].Message != "GET /settings" {
		t.Errorf("breadcrumbs = %+v", event.Breadcrumbs)
	}
}
