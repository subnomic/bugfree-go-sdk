package bugfree

import (
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Span is one timed piece of work. The root span of a request or a job is its
// transaction; the steps inside it (a query, an outgoing call) are child spans.
//
// A nil *Span is valid and does nothing, so code can time itself whether tracing
// is on or not.
type Span struct {
	client *Client

	TraceID      string
	SpanID       string
	ParentSpanID string
	Op           string
	// Name is the transaction's name ("GET /orders/:id"); Description a child
	// span's ("SELECT * FROM orders WHERE id = $1").
	Name        string
	Description string
	Status      string
	HTTPStatus  int
	Start       time.Time

	sampled     bool
	transaction *Span

	mu       sync.Mutex
	finished bool
	duration time.Duration
	data     map[string]any
	tags     map[string]any
	children []*Span
}

// maxChildSpans bounds the spans one transaction records; a loop of queries
// would otherwise grow it without end.
const maxChildSpans = 1000

// SpanOption configures a new transaction.
type SpanOption func(*Span)

// WithOp sets the kind of work: http.server, task, queue.process.
func WithOp(op string) SpanOption {
	return func(span *Span) { span.Op = op }
}

// ContinueTrace makes the transaction part of the trace a W3C traceparent header
// names ("00-<trace id>-<parent span id>-<flags>"), so the services of one request
// show as one trace. An empty or malformed header starts a new trace.
func ContinueTrace(traceparent string) SpanOption {
	return func(span *Span) {
		traceID, parentID, sampled, ok := parseTraceparent(traceparent)
		if !ok {
			return
		}
		span.TraceID, span.ParentSpanID = traceID, parentID
		span.sampled = sampled
	}
}

type spanKey struct{}

// SpanFromContext returns the span ctx carries, or nil.
func SpanFromContext(ctx context.Context) *Span {
	if ctx == nil {
		return nil
	}
	span, _ := ctx.Value(spanKey{}).(*Span)
	return span
}

// TracingEnabled reports whether transactions are recorded.
func (c *Client) TracingEnabled() bool {
	return c.Enabled() && c.options.TracesSampleRate > 0
}

// StartTransaction begins a transaction and returns a context carrying it. With
// tracing off it returns ctx as it is and a nil span.
//
//	ctx, transaction := bugfree.StartTransaction(ctx, "nightly import", bugfree.WithOp("task"))
//	defer transaction.Finish()
func (c *Client) StartTransaction(ctx context.Context, name string, options ...SpanOption) (context.Context, *Span) {
	if !c.TracingEnabled() {
		return ctx, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	span := &Span{client: c, Name: name, Start: time.Now(), Status: "ok", SpanID: newSpanID()}
	span.transaction = span
	decided := false
	for _, option := range options {
		option(span)
	}
	if span.TraceID != "" {
		decided = true
	} else {
		span.TraceID = newTraceID()
	}
	if !decided {
		span.sampled = c.sampleTrace()
	}
	return context.WithValue(ctx, spanKey{}, span), span
}

// StartTransaction begins a transaction with the global client.
func StartTransaction(ctx context.Context, name string, options ...SpanOption) (context.Context, *Span) {
	return Current().StartTransaction(ctx, name, options...)
}

// StartSpan begins a child of the span ctx carries. Without one it returns ctx
// as it is and a nil span.
//
//	ctx, span := bugfree.StartSpan(ctx, "cache.get", key)
//	defer span.Finish()
func StartSpan(ctx context.Context, op, description string) (context.Context, *Span) {
	parent := SpanFromContext(ctx)
	if parent == nil {
		return ctx, nil
	}
	span := &Span{
		client:       parent.client,
		TraceID:      parent.TraceID,
		SpanID:       newSpanID(),
		ParentSpanID: parent.SpanID,
		Op:           op,
		Description:  description,
		Status:       "ok",
		Start:        time.Now(),
		sampled:      parent.sampled,
		transaction:  parent.transaction,
	}
	return context.WithValue(ctx, spanKey{}, span), span
}

// SetName renames a transaction, once the handler knows its route.
func (s *Span) SetName(name string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Name = name
}

// SetStatus sets how the work ended: "ok", or an error such as "internal_error".
func (s *Span) SetStatus(status string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Status = status
}

// SetHTTPStatus records the response status, and an error status for a 5xx.
func (s *Span) SetHTTPStatus(code int) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.HTTPStatus = code
	if code >= 500 {
		s.Status = "internal_error"
	}
}

// SetData attaches a value to the span.
func (s *Span) SetData(key string, value any) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data == nil {
		s.data = map[string]any{}
	}
	s.data[key] = value
}

// SetTag attaches a searchable tag to a transaction.
func (s *Span) SetTag(key string, value any) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.tags == nil {
		s.tags = map[string]any{}
	}
	s.tags[key] = value
}

// Traceparent renders the W3C header that continues this span's trace in the
// service a request goes to.
func (s *Span) Traceparent() string {
	if s == nil {
		return ""
	}
	flags := "00"
	if s.sampled {
		flags = "01"
	}
	return fmt.Sprintf("00-%s-%s-%s", s.TraceID, s.SpanID, flags)
}

// Finish ends the span. Finishing a transaction reports it, with the child spans
// finished by then; a sampled-out transaction is not reported.
func (s *Span) Finish() {
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.finished {
		s.mu.Unlock()
		return
	}
	s.finished = true
	s.duration = time.Since(s.Start)
	s.mu.Unlock()

	if s.transaction != s {
		s.transaction.addChild(s)
		return
	}
	if s.sampled {
		s.client.sendTransaction(s.payload())
	}
}

func (s *Span) addChild(child *Span) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.finished && len(s.children) < maxChildSpans {
		s.children = append(s.children, child)
	}
}

// ---------- delivery ----------

// transactionPayload is a transaction as the server reads it.
type transactionPayload struct {
	TraceID      string         `json:"trace_id"`
	SpanID       string         `json:"span_id"`
	ParentSpanID string         `json:"parent_span_id,omitempty"`
	Name         string         `json:"name"`
	Op           string         `json:"op,omitempty"`
	Status       string         `json:"status"`
	HTTPStatus   int            `json:"http_status,omitempty"`
	Start        string         `json:"start"`
	DurationMS   float64        `json:"duration_ms"`
	Environment  string         `json:"environment,omitempty"`
	Release      string         `json:"release,omitempty"`
	Platform     string         `json:"platform"`
	Tags         map[string]any `json:"tags,omitempty"`
	Spans        []spanPayload  `json:"spans"`
}

type spanPayload struct {
	SpanID       string         `json:"span_id"`
	ParentSpanID string         `json:"parent_span_id"`
	Op           string         `json:"op,omitempty"`
	Description  string         `json:"description,omitempty"`
	Status       string         `json:"status,omitempty"`
	Start        string         `json:"start"`
	DurationMS   float64        `json:"duration_ms"`
	Data         map[string]any `json:"data,omitempty"`
}

func milliseconds(duration time.Duration) float64 {
	return float64(duration) / float64(time.Millisecond)
}

func (s *Span) payload() *transactionPayload {
	s.mu.Lock()
	defer s.mu.Unlock()

	payload := &transactionPayload{
		TraceID:      s.TraceID,
		SpanID:       s.SpanID,
		ParentSpanID: s.ParentSpanID,
		Name:         s.Name,
		Op:           s.Op,
		Status:       s.Status,
		HTTPStatus:   s.HTTPStatus,
		Start:        timestamp(s.Start),
		DurationMS:   milliseconds(s.duration),
		Environment:  s.client.options.Environment,
		Release:      s.client.options.Release,
		Platform:     "go",
		Tags:         s.tags,
		Spans:        make([]spanPayload, 0, len(s.children)),
	}
	for _, child := range s.children {
		child.mu.Lock()
		payload.Spans = append(payload.Spans, spanPayload{
			SpanID:       child.SpanID,
			ParentSpanID: child.ParentSpanID,
			Op:           child.Op,
			Description:  child.Description,
			Status:       child.Status,
			Start:        timestamp(child.Start),
			DurationMS:   milliseconds(child.duration),
			Data:         child.data,
		})
		child.mu.Unlock()
	}
	return payload
}

// TransactionSender is a Transport that also delivers transactions, as the SDK's
// own test transports do.
type TransactionSender interface {
	SendTransaction(transaction []byte)
}

// transactionQueueSize bounds the transactions waiting to be sent; past it they
// are dropped rather than slowing the application down.
const transactionQueueSize = 100

// sendTransaction queues a finished transaction for the background sender.
func (c *Client) sendTransaction(payload *transactionPayload) {
	body, err := json.Marshal(map[string]any{"transactions": []*transactionPayload{payload}})
	if err != nil {
		return
	}
	if sender, ok := c.transport.(TransactionSender); ok {
		sender.SendTransaction(body)
		return
	}
	c.tracesMu.RLock()
	defer c.tracesMu.RUnlock()
	if c.tracesClosed {
		return
	}
	c.tracesOnce.Do(func() {
		c.traces = make(chan []byte, transactionQueueSize)
		c.tracesDone = make(chan struct{})
		go c.deliverTransactions()
	})
	select {
	case c.traces <- body:
	default:
		c.stats.transactions.Add(1)
	}
}

// closeTraces stops taking transactions and gives the queued ones a moment to go
// out, bounded like the event queue.
func (c *Client) closeTraces() {
	c.tracesMu.Lock()
	if c.tracesClosed {
		c.tracesMu.Unlock()
		return
	}
	c.tracesClosed = true
	queue, done := c.traces, c.tracesDone
	c.tracesMu.Unlock()
	if queue == nil {
		return
	}
	close(queue)
	select {
	case <-done:
	case <-time.After(c.options.ShutdownTimeout):
	}
}

// deliverTransactions sends the queued transactions one after another.
func (c *Client) deliverTransactions() {
	defer close(c.tracesDone)
	for body := range c.traces {
		if transport, ok := c.transport.(*httpTransport); ok && transport.paused() {
			continue
		}
		if err := c.postTransaction(body); err != nil && c.options.Debug {
			log.Printf("bugfree: a transaction could not be sent: %v", err)
		}
	}
}

func (c *Client) postTransaction(body []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), checkInTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.dsn.base+"/transactions", bytes.NewReader(body))
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
	if response.StatusCode == http.StatusTooManyRequests {
		if transport, ok := c.transport.(*httpTransport); ok {
			transport.pause(parseRetryAfter(response.Header.Get("Retry-After"), time.Now()))
		}
	}
	if response.StatusCode >= 300 {
		return fmt.Errorf("status %d", response.StatusCode)
	}
	return nil
}

// sampleTrace decides whether a new trace is recorded.
func (c *Client) sampleTrace() bool {
	if c.options.TracesSampleRate >= 1 {
		return true
	}
	c.randomMu.Lock()
	defer c.randomMu.Unlock()
	return c.random.Float64() < c.options.TracesSampleRate
}

// ---------- ids and headers ----------

func randomHex(bytes int) string {
	buffer := make([]byte, bytes)
	if _, err := cryptorand.Read(buffer); err != nil {
		return strings.Repeat("0", bytes*2)
	}
	return hex.EncodeToString(buffer)
}

func newTraceID() string { return randomHex(16) }
func newSpanID() string  { return randomHex(8) }

// parseTraceparent reads a W3C traceparent header.
func parseTraceparent(header string) (traceID, parentID string, sampled, ok bool) {
	parts := strings.Split(strings.TrimSpace(strings.ToLower(header)), "-")
	if len(parts) < 4 || len(parts[0]) != 2 || parts[0] == "ff" {
		return "", "", false, false
	}
	traceID, parentID, flags := parts[1], parts[2], parts[3]
	if !isHex(traceID, 32) || !isHex(parentID, 16) || !isHex(flags, 2) ||
		traceID == strings.Repeat("0", 32) || parentID == strings.Repeat("0", 16) {
		return "", "", false, false
	}
	flagBits, _ := hex.DecodeString(flags)
	return traceID, parentID, flagBits[0]&1 == 1, true
}

func isHex(value string, length int) bool {
	if len(value) != length {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

// withTraceOf ties an event to the trace of the span ctx carries.
func withTraceOf(ctx context.Context) EventModifier {
	span := SpanFromContext(ctx)
	if span == nil {
		return nil
	}
	return func(event *Event) {
		if event.TraceID == "" {
			event.TraceID = span.TraceID
		}
		if event.Tags == nil {
			event.Tags = map[string]any{}
		}
		event.Tags["span_id"] = span.SpanID
	}
}
