package bugfree

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// traceTransport records events and transactions.
type traceTransport struct {
	recordingTransport
	mu           sync.Mutex
	transactions []transactionPayload
}

func (t *traceTransport) SendTransaction(body []byte) {
	var report struct {
		Transactions []transactionPayload `json:"transactions"`
	}
	_ = json.Unmarshal(body, &report)
	t.mu.Lock()
	defer t.mu.Unlock()
	t.transactions = append(t.transactions, report.Transactions...)
}

func (t *traceTransport) all() []transactionPayload {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]transactionPayload(nil), t.transactions...)
}

func newTracingClient(t *testing.T, rate float64) (*Client, *traceTransport) {
	t.Helper()
	transport := &traceTransport{}
	client, err := NewClient(Options{DSN: "http://key@localhost/ingest", Release: "api@2", Transport: transport, TracesSampleRate: rate, DedupeWindow: -1})
	if err != nil {
		t.Fatal(err)
	}
	installForTest(t, client)
	return client, transport
}

func TestParseTraceparent(t *testing.T) {
	traceID, parentID, sampled, ok := parseTraceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	if !ok || traceID != "4bf92f3577b34da6a3ce929d0e0e4736" || parentID != "00f067aa0ba902b7" || !sampled {
		t.Errorf("parsed %q %q %v %v", traceID, parentID, sampled, ok)
	}
	for _, header := range []string{
		"", "garbage", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7",
		"ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		"00-00000000000000000000000000000000-00f067aa0ba902b7-01",
		"00-4bf92f3577b34da6a3ce929d0e0e473-00f067aa0ba902b7-01",
	} {
		if _, _, _, ok := parseTraceparent(header); ok {
			t.Errorf("%q was accepted", header)
		}
	}
}

func TestMiddlewareRecordsATransactionWithChildSpans(t *testing.T) {
	client, transport := newTracingClient(t, 1)

	name := "fake+traced+" + t.Name()
	sql.Register(name, WrapDriver(&fakeDriver{}))
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var downstreamTraceparent string
	downstream := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		downstreamTraceparent = r.Header.Get("traceparent")
	}))
	defer downstream.Close()
	httpClient := &http.Client{Transport: BreadcrumbTransport(nil)}

	handler := Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = db.ExecContext(r.Context(), "UPDATE orders SET paid = true")
		request, _ := http.NewRequestWithContext(r.Context(), http.MethodGet, downstream.URL+"/rates?token=x", nil)
		if response, err := httpClient.Do(request); err == nil {
			response.Body.Close()
		}
		CaptureExceptionContext(r.Context(), errors.New("partial failure"))
		w.WriteHeader(http.StatusCreated)
	}))

	incoming := httptest.NewRequest(http.MethodPost, "/orders", nil)
	incoming.Header.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	handler.ServeHTTP(httptest.NewRecorder(), incoming)

	transactions := transport.all()
	if len(transactions) != 1 {
		t.Fatalf("got %d transactions", len(transactions))
	}
	transaction := transactions[0]
	if transaction.TraceID != "4bf92f3577b34da6a3ce929d0e0e4736" || transaction.ParentSpanID != "00f067aa0ba902b7" {
		t.Errorf("the incoming trace was not continued: %+v", transaction)
	}
	if transaction.Name != "POST /orders" || transaction.Op != "http.server" || transaction.HTTPStatus != 201 || transaction.Status != "ok" {
		t.Errorf("transaction = %+v", transaction)
	}

	ops := map[string]spanPayload{}
	for _, span := range transaction.Spans {
		ops[span.Op] = span
		if span.ParentSpanID != transaction.SpanID {
			t.Errorf("span %s hangs off %s, expected the transaction", span.Op, span.ParentSpanID)
		}
	}
	if ops["db.query"].Description != "UPDATE orders SET paid = true" {
		t.Errorf("db span = %+v", ops["db.query"])
	}
	if !strings.HasSuffix(ops["http.client"].Description, "/rates") {
		t.Errorf("http span = %+v", ops["http.client"])
	}
	if !strings.Contains(downstreamTraceparent, "4bf92f3577b34da6a3ce929d0e0e4736-"+ops["http.client"].SpanID) {
		t.Errorf("downstream traceparent = %q, expected the http span's", downstreamTraceparent)
	}

	if event := transport.last(); event == nil || event.TraceID != transaction.TraceID {
		t.Errorf("the error was not tied to the trace: %+v", event)
	}
	_ = client
}

func TestUnsampledTraceIsNotSentButStillPropagates(t *testing.T) {
	_, transport := newTracingClient(t, 1)

	ctx, transaction := StartTransaction(context.Background(), "job", ContinueTrace("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00"))
	if !strings.HasSuffix(transaction.Traceparent(), "-00") || SpanFromContext(ctx) != transaction {
		t.Errorf("traceparent = %q", transaction.Traceparent())
	}
	transaction.Finish()
	if len(transport.all()) != 0 {
		t.Error("a trace the caller did not sample was sent")
	}
}

func TestTracingOffLeavesNilSpansThatAreSafe(t *testing.T) {
	client, _ := newTestClient(t, nil)
	ctx, transaction := client.StartTransaction(context.Background(), "job")
	if transaction != nil {
		t.Fatal("tracing is off without a rate")
	}
	_, span := StartSpan(ctx, "db.query", "SELECT 1")
	span.SetData("rows", 1)
	span.SetStatus("ok")
	span.Finish()
	transaction.SetHTTPStatus(500)
	transaction.Finish()
}

func TestChildrenFinishedAfterTheTransactionAreDropped(t *testing.T) {
	_, transport := newTracingClient(t, 1)
	ctx, transaction := StartTransaction(context.Background(), "job")
	_, early := StartSpan(ctx, "step", "early")
	_, late := StartSpan(ctx, "step", "late")
	early.Finish()
	transaction.Finish()
	late.Finish()

	sent := transport.all()
	if len(sent) != 1 || len(sent[0].Spans) != 1 || sent[0].Spans[0].Description != "early" {
		t.Errorf("sent = %+v", sent)
	}
}
