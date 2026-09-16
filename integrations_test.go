package bugfree

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestGoRecordsPanicWithTheCallersScope(t *testing.T) {
	client, transport := newTestClient(t, nil)

	ctx := client.ContextWithScope(context.Background())
	ScopeFromContext(ctx).SetUser(User{ID: "alice"})

	done := make(chan struct{})
	client.Go(ctx, func(ctx context.Context) {
		defer close(done)
		// The goroutine's own scope: what it sets stays with it.
		ScopeFromContext(ctx).SetTag("job", "resize")
		panic("image is empty")
	})
	<-done
	waitFor(t, func() bool { return transport.last() != nil })

	event := transport.last()
	if event.Level != LevelFatal || event.Message != "panic: image is empty" {
		t.Errorf("event = %s %q", event.Level, event.Message)
	}
	if event.User == nil || event.User.ID != "alice" || event.Tags["job"] != "resize" {
		t.Errorf("user = %+v, tags = %v, expected the inherited user and the goroutine's tag", event.User, event.Tags)
	}
}

// waitFor polls a condition that another goroutine makes true.
func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition was not met in time")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestLogHandlerTurnsErrorsIntoEventsAndTheRestIntoBreadcrumbs(t *testing.T) {
	client, transport := newTestClient(t, nil)

	var written bytes.Buffer
	logger := slog.New(NewLogHandler(slog.NewTextHandler(&written, nil), LogHandlerOptions{Client: client})).
		With("service", "billing")

	ctx := client.ContextWithScope(context.Background())
	logger.InfoContext(ctx, "charging", "order", 42)
	logger.DebugContext(ctx, "not recorded")
	cause := errors.New("card declined")
	logger.ErrorContext(ctx, "charge failed", "order", 42, slog.Group("card", "brand", "visa"), "err", cause)

	if !strings.Contains(written.String(), "charge failed") {
		t.Error("the record did not reach the wrapped handler")
	}

	event := transport.last()
	if event == nil {
		t.Fatal("the error record produced no event")
	}
	if event.Type != "*errors.errorString" || event.Message != "charge failed: card declined" {
		t.Errorf("event = %s %q", event.Type, event.Message)
	}
	if event.Extra["service"] != "billing" || event.Extra["card.brand"] != "visa" || event.Extra["err"] != "card declined" {
		t.Errorf("extra = %v", event.Extra)
	}
	if len(event.Breadcrumbs) != 1 || event.Breadcrumbs[0].Message != "charging" || event.Breadcrumbs[0].Data != "order=42 service=billing" {
		t.Errorf("breadcrumbs = %+v", event.Breadcrumbs)
	}
	if len(event.Stacktrace) == 0 || !strings.HasSuffix(event.Stacktrace[0].Function, "TestLogHandlerTurnsErrorsIntoEventsAndTheRestIntoBreadcrumbs") {
		t.Errorf("the stack should start at the logging call, got %+v", event.Stacktrace)
	}

	// The breadcrumb went to the request's scope, not the global one.
	if crumbs := client.Scope().breadcrumbs(); len(crumbs) != 0 {
		t.Errorf("global breadcrumbs = %+v, expected none", crumbs)
	}
}

func TestLogHandlerLevelsAreConfigurable(t *testing.T) {
	client, transport := newTestClient(t, nil)
	logger := slog.New(NewLogHandler(nil, LogHandlerOptions{
		Client:          client,
		EventLevel:      slog.LevelWarn,
		BreadcrumbLevel: slog.LevelError + 4,
	}))

	logger.Info("ignored")
	logger.Warn("disk almost full")

	event := transport.last()
	if event == nil || event.Level != LevelWarning || event.Type != "log" {
		t.Fatalf("event = %+v, expected a warning", event)
	}
	if len(event.Breadcrumbs) != 0 {
		t.Errorf("breadcrumbs = %+v, expected none", event.Breadcrumbs)
	}
}

func TestBreadcrumbTransportRecordsRequestsWithoutQuery(t *testing.T) {
	client, _ := newTestClient(t, nil)
	installForTest(t, client)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer server.Close()

	httpClient := &http.Client{Transport: BreadcrumbTransport(nil)}
	ctx := client.ContextWithScope(context.Background())
	request, _ := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/rates?token=secret", nil)
	response, err := httpClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	response.Body.Close()

	crumbs := ScopeFromContext(ctx).breadcrumbs()
	if len(crumbs) != 1 {
		t.Fatalf("breadcrumbs = %+v, expected one", crumbs)
	}
	if crumbs[0].Message != "GET "+server.URL+"/rates" || crumbs[0].Level != "error" || !strings.HasPrefix(crumbs[0].Data, "502 · ") {
		t.Errorf("breadcrumb = %+v", crumbs[0])
	}
}

func TestWrapDriverRecordsQueriesWithoutArguments(t *testing.T) {
	client, _ := newTestClient(t, nil)
	installForTest(t, client)

	name := "fake+bugfree+" + t.Name()
	fake := &fakeDriver{}
	sql.Register(name, WrapDriver(fake))
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := client.ContextWithScope(context.Background())
	if _, err := db.ExecContext(ctx, "UPDATE orders\n   SET status = $1 WHERE id = $2", "paid", 7); err != nil {
		t.Fatal(err)
	}
	fake.fail.Store(true)
	if _, err := db.QueryContext(ctx, "SELECT * FROM orders WHERE email = $1", "jane@example.com"); err == nil {
		t.Fatal("the failing query returned no error")
	}

	crumbs := ScopeFromContext(ctx).breadcrumbs()
	if len(crumbs) != 2 {
		t.Fatalf("breadcrumbs = %+v, expected two", crumbs)
	}
	failed, succeeded := crumbs[0], crumbs[1]
	if succeeded.Category != "sql" || succeeded.Message != "UPDATE orders SET status = $1 WHERE id = $2" || succeeded.Level != "info" {
		t.Errorf("succeeded = %+v", succeeded)
	}
	if failed.Level != "error" || !strings.Contains(failed.Data, "connection reset") {
		t.Errorf("failed = %+v", failed)
	}
	for _, crumb := range crumbs {
		if strings.Contains(crumb.Message+crumb.Data, "jane@example.com") || strings.Contains(crumb.Message+crumb.Data, "paid") {
			t.Errorf("an argument leaked into %+v", crumb)
		}
	}
}

// fakeDriver is the smallest context-aware driver.
type fakeDriver struct {
	fail atomic.Bool
}

func (d *fakeDriver) Open(string) (driver.Conn, error) { return &fakeConn{driver: d}, nil }

type fakeConn struct {
	driver *fakeDriver
}

func (c *fakeConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not supported") }
func (c *fakeConn) Close() error                        { return nil }
func (c *fakeConn) Begin() (driver.Tx, error)           { return nil, errors.New("not supported") }

func (c *fakeConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	return driver.RowsAffected(1), nil
}

func (c *fakeConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	if c.driver.fail.Load() {
		return nil, errors.New("connection reset by peer")
	}
	return nil, errors.New("no rows in the fake")
}
