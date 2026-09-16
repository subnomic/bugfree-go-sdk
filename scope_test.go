package bugfree

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

// Two requests in flight at once must not see each other's user: with one
// global scope the second SetUser overwrote the first, and an error of the first
// request was reported against the second request's user.
func TestContextScopesKeepConcurrentUsersApart(t *testing.T) {
	client, transport := newTestClient(t, nil)

	alice := client.ContextWithScope(context.Background())
	bob := client.ContextWithScope(context.Background())

	ScopeFromContext(alice).SetUser(User{ID: "alice"})
	ScopeFromContext(bob).SetUser(User{ID: "bob"})

	client.CaptureExceptionContext(alice, errors.New("alice failed"))
	if got := transport.last().User; got == nil || got.ID != "alice" {
		t.Fatalf("user = %+v, expected alice", got)
	}

	client.CaptureExceptionContext(bob, errors.New("bob failed"))
	if got := transport.last().User; got == nil || got.ID != "bob" {
		t.Fatalf("user = %+v, expected bob", got)
	}

	// The global scope stays untouched.
	client.CaptureException(errors.New("background job failed"))
	if got := transport.last().User; got != nil {
		t.Fatalf("user = %+v, expected none on the global scope", got)
	}
}

func TestContextScopeInheritsFromGlobalScope(t *testing.T) {
	client, transport := newTestClient(t, nil)

	client.Scope().SetTag("region", "eu")
	client.Scope().SetUser(User{ID: "service-account"})
	client.Scope().AddBreadcrumb(Breadcrumb{Category: "boot", Message: "started", At: "2026-01-01T10:00:00Z"})

	ctx := client.ContextWithScope(context.Background())
	scope := ScopeFromContext(ctx)
	scope.SetTag("route", "/orders")
	scope.AddBreadcrumb(Breadcrumb{Category: "sql", Message: "select orders", At: "2026-01-01T10:00:05Z"})

	// Set after the request started: inheritance is live.
	client.Scope().SetTag("build", "42")

	client.CaptureExceptionContext(ctx, errors.New("boom"))
	event := transport.last()

	if event.Tags["region"] != "eu" || event.Tags["route"] != "/orders" || event.Tags["build"] != "42" {
		t.Errorf("tags = %v, expected the inherited and the own tags", event.Tags)
	}
	if event.User == nil || event.User.ID != "service-account" {
		t.Errorf("user = %+v, expected the inherited user", event.User)
	}
	// The global scope's steps belong to every other request; they stay out.
	if len(event.Breadcrumbs) != 1 || event.Breadcrumbs[0].Message != "select orders" {
		t.Errorf("breadcrumbs = %+v, expected only the request's own step", event.Breadcrumbs)
	}

	// The request's own values never leak back to the global scope.
	client.CaptureException(errors.New("elsewhere"))
	if _, leaked := transport.last().Tags["route"]; leaked {
		t.Error("a request tag reached the global scope")
	}
}

// A scope started from a request (a goroutine of it, say) continues the same flow
// and does inherit the request's steps.
func TestChildScopeKeepsNewestBreadcrumbsWithinLimit(t *testing.T) {
	parent := newGlobalScope(3).child()
	child := parent.child()

	parent.AddBreadcrumb(Breadcrumb{Message: "p1", At: "2026-01-01T10:00:01Z"})
	child.AddBreadcrumb(Breadcrumb{Message: "c2", At: "2026-01-01T10:00:02Z"})
	parent.AddBreadcrumb(Breadcrumb{Message: "p3", At: "2026-01-01T10:00:03Z"})
	child.AddBreadcrumb(Breadcrumb{Message: "c4", At: "2026-01-01T10:00:04Z"})

	got := child.breadcrumbs()
	want := []string{"c4", "p3", "c2"}
	if len(got) != len(want) {
		t.Fatalf("got %d steps, expected %d", len(got), len(want))
	}
	for i, message := range want {
		if got[i].Message != message {
			t.Errorf("step %d = %q, expected %q", i, got[i].Message, message)
		}
	}
}

func TestNilScopeIsSafe(t *testing.T) {
	var scope *Scope
	scope.SetUser(User{ID: "x"})
	scope.SetTag("k", "v")
	scope.AddBreadcrumb(Breadcrumb{Message: "m"})

	if ScopeFromContext(context.Background()) != nil {
		t.Error("a bare context must carry no scope")
	}
}

func TestMiddlewareGivesEveryRequestItsOwnScope(t *testing.T) {
	client, transport := newTestClient(t, nil)
	installForTest(t, client)

	var wg sync.WaitGroup
	handler := Middleware(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		user := r.URL.Query().Get("user")
		ScopeFromContext(r.Context()).SetUser(User{ID: user})
		// Both requests are inside the handler before either reports.
		wg.Wait()
		CaptureExceptionContext(r.Context(), fmt.Errorf("failed for %s", user))
	}))

	wg.Add(1)
	done := make(chan struct{}, 2)
	for _, user := range []string{"alice", "bob"} {
		go func() {
			handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/?user="+user, nil))
			done <- struct{}{}
		}()
	}
	time.Sleep(50 * time.Millisecond)
	wg.Done()
	<-done
	<-done

	transport.mu.Lock()
	defer transport.mu.Unlock()
	if len(transport.events) != 2 {
		t.Fatalf("got %d events, expected 2", len(transport.events))
	}
	for _, event := range transport.events {
		if event.Message != "failed for "+event.User.ID {
			t.Errorf("event %q carries user %q", event.Message, event.User.ID)
		}
	}
}

// installForTest makes client the global one for the length of the test.
func installForTest(t *testing.T, client *Client) {
	t.Helper()
	currentMu.Lock()
	previous := current
	current = client
	currentMu.Unlock()

	t.Cleanup(func() {
		currentMu.Lock()
		current = previous
		currentMu.Unlock()
	})
}

func TestCaptureExceptionListsWrappedErrors(t *testing.T) {
	client, transport := newTestClient(t, nil)

	root := &fs.PathError{Op: "open", Path: "/etc/app.yaml", Err: fs.ErrNotExist}
	err := fmt.Errorf("load settings: %w", root)

	client.CaptureException(err)
	chain, ok := transport.last().Extra["error_chain"].([]map[string]string)
	if !ok {
		t.Fatalf("extra = %v, expected an error chain", transport.last().Extra)
	}

	// The event itself is the outer error; the chain holds what it wraps.
	if len(chain) != 2 {
		t.Fatalf("chain = %v, expected the path error and its cause", chain)
	}
	if chain[0]["type"] != "*fs.PathError" || chain[1]["message"] != "file does not exist" {
		t.Errorf("chain = %v", chain)
	}
}

func TestCaptureExceptionFollowsJoinedErrors(t *testing.T) {
	client, transport := newTestClient(t, nil)

	client.CaptureException(errors.Join(errors.New("cache down"), errors.New("database down")))
	chain, _ := transport.last().Extra["error_chain"].([]map[string]string)

	if len(chain) != 2 || chain[0]["message"] != "cache down" || chain[1]["message"] != "database down" {
		t.Errorf("chain = %v, expected both joined errors", chain)
	}
}

func TestPlainErrorHasNoChain(t *testing.T) {
	client, transport := newTestClient(t, nil)

	client.CaptureException(errors.New("plain"))
	if _, ok := transport.last().Extra["error_chain"]; ok {
		t.Error("an error that wraps nothing must not carry a chain")
	}
}

var uuidV4 = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func TestCaptureReturnsTheSentEventID(t *testing.T) {
	client, transport := newTestClient(t, nil)

	id := client.CaptureException(errors.New("boom"))
	if !uuidV4.MatchString(id) {
		t.Fatalf("id = %q, expected a version 4 UUID", id)
	}
	if transport.last().EventID != id {
		t.Errorf("sent event id = %q, returned %q", transport.last().EventID, id)
	}
	if other := client.CaptureMessage(LevelInfo, "hello"); other == id || other == "" {
		t.Errorf("second id = %q, expected a new one", other)
	}
}

func TestDroppedEventReturnsNoID(t *testing.T) {
	client, _ := newTestClient(t, func(options *Options) {
		options.BeforeSend = func(*Event) *Event { return nil }
	})
	if id := client.CaptureException(errors.New("dropped")); id != "" {
		t.Errorf("id = %q, expected empty for a dropped event", id)
	}

	disabled, _ := NewClient(Options{})
	if id := disabled.CaptureException(errors.New("off")); id != "" {
		t.Errorf("id = %q, expected empty for a disabled client", id)
	}
}

func TestMiddlewareStripsQueryLogsAndLetsAbortThrough(t *testing.T) {
	client, transport := newTestClient(t, nil)
	installForTest(t, client)

	var logged strings.Builder
	previous := log.Writer()
	log.SetOutput(&logged)
	defer log.SetOutput(previous)

	handler := Middleware(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/abort" {
			panic(http.ErrAbortHandler)
		}
		panic("boom")
	}))

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/reset?token=secret#part", nil))
	if recorder.Code != http.StatusInternalServerError {
		t.Errorf("status = %d", recorder.Code)
	}
	if url := transport.last().Request.URL; url != "/reset" {
		t.Errorf("url = %q, expected no query string", url)
	}
	if !strings.Contains(logged.String(), "panic recovered: boom") {
		t.Errorf("log = %q, expected the swallowed panic", logged.String())
	}

	func() {
		defer func() {
			if recovered := recover(); recovered != http.ErrAbortHandler {
				t.Errorf("recovered = %v, expected http.ErrAbortHandler", recovered)
			}
		}()
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/abort", nil))
	}()
	transport.mu.Lock()
	defer transport.mu.Unlock()
	if len(transport.events) != 1 {
		t.Errorf("got %d events, an abort must not be recorded", len(transport.events))
	}
}
