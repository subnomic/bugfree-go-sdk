package bugfree

import (
	"context"
	cryptorand "crypto/rand"
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime"
	"runtime/metrics"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Version is the SDK version; used in the User-Agent and the event tags.
const Version = "0.6.0"

// Client collects the events and hands them to the transport.
//
// It is safe for concurrent use: the scope (breadcrumbs, tags, user) is guarded
// by a lock and sending happens on a separate goroutine.
type Client struct {
	options Options
	dsn     *dsn
	source  *sourceReader

	transport Transport
	scope     *Scope
	random    *rand.Rand
	randomMu  sync.Mutex

	dedupe *dedupe
	stats  clientStats

	// sessions counts request sessions; nil unless TrackSessions is on.
	sessions *sessionAggregator

	// profiler takes profiles; nil unless ProfilingInterval is set.
	profiler *profiler

	// traces queues finished transactions; started with the first one.
	traces       chan []byte
	tracesOnce   sync.Once
	tracesMu     sync.RWMutex
	tracesClosed bool
	tracesDone   chan struct{}
}

// current is the global client; Init installs it.
var (
	current   *Client
	currentMu sync.RWMutex
)

// Init installs the global client.
//
// An empty DSN is not an error: monitoring counts as off and every Capture call
// is silently ignored. That way application code is identical in every environment.
func Init(options Options) error {
	client, err := NewClient(options)
	if err != nil {
		return err
	}

	currentMu.Lock()
	previous := current
	current = client
	currentMu.Unlock()

	if previous != nil {
		previous.Close()
	}
	return nil
}

// NewClient builds a standalone client (for sending to more than one project).
func NewClient(options Options) (*Client, error) {
	options.normalize()

	client := &Client{
		options: options,
		scope:   newGlobalScope(options.MaxBreadcrumbs),
		source:  newSourceReader(options.SourceRoots, options.SourceFS),
		random:  rand.New(rand.NewSource(time.Now().UnixNano())),
		dedupe:  newDedupe(options.DedupeWindow),
	}

	if options.ServerName == "" {
		if hostname, err := os.Hostname(); err == nil {
			client.options.ServerName = hostname
		}
	}

	if strings.TrimSpace(options.DSN) == "" {
		// Monitoring is off; no transport is built.
		if options.Debug {
			log.Print("bugfree: DSN is empty, monitoring is disabled")
		}
		return client, nil
	}

	parsed, err := parseDSN(options.DSN)
	if err != nil {
		return nil, err
	}
	client.dsn = parsed

	if options.Transport != nil {
		client.transport = options.Transport
	} else {
		transport := newHTTPTransport(parsed.storeURL, options.HTTPClient, options.Debug)
		transport.closeTimeout = client.options.ShutdownTimeout
		client.transport = transport
	}
	if options.TrackSessions {
		client.sessions = newSessionAggregator(client)
	}
	if options.ProfilingInterval > 0 {
		client.profiler = newProfiler(client, options.ProfilingInterval, options.ProfileDuration)
	}
	return client, nil
}

// Enabled reports whether the client can send events.
func (c *Client) Enabled() bool {
	return c != nil && c.transport != nil
}

// Scope returns the client's scope.
func (c *Client) Scope() *Scope {
	if c == nil {
		return nil
	}
	return c.scope
}

// Close reports the counted sessions and shuts the transport down.
func (c *Client) Close() {
	if c == nil {
		return
	}
	if c.profiler != nil {
		c.profiler.close()
	}
	if c.sessions != nil {
		c.sessions.close()
	}
	c.closeTraces()
	if c.transport != nil {
		c.transport.Close()
	}
}

// CaptureException records an error and returns the event's id.
//
// The id is empty when nothing is sent: the client is disabled, or sampling,
// deduplication or BeforeSend dropped the event. Show it to the user ("error
// reference: ...") to find the event again.
//
// The stack is the one recorded where the error was created (WithStack, Errorf)
// when the error carries one, and the caller's otherwise.
func (c *Client) CaptureException(err error, modifiers ...EventModifier) string {
	if !c.Enabled() || err == nil || !c.sampled() {
		return ""
	}

	frames := errorFrames(err)
	if frames == nil {
		frames = captureStack(1)
	}
	return c.captureError(c.scope, err, frames, modifiers)
}

// CaptureExceptionContext records an error with the scope ctx carries.
//
// Use it inside a request: the request's user, tags and breadcrumbs are attached
// instead of the global scope's alone.
func (c *Client) CaptureExceptionContext(ctx context.Context, err error, modifiers ...EventModifier) string {
	if !c.Enabled() || err == nil || !c.sampled() {
		return ""
	}

	frames := errorFrames(err)
	if frames == nil {
		frames = captureStack(1)
	}
	return c.captureError(c.scopeFor(ctx), err, frames, withTrace(ctx, modifiers))
}

// CaptureExceptionWithStack records an error with the call stack captured where
// it was created.
//
// For returned (not thrown) errors the stack is meaningful where the error was
// created, not where it was handled: what you look for behind "the settings
// could not be updated" is the service function that produced it, not the HTTP
// layer that reported it. callers is collected with runtime.Callers at the point
// the error was created. WithStack and Errorf do that for you.
func (c *Client) CaptureExceptionWithStack(err error, callers []uintptr, modifiers ...EventModifier) string {
	if !c.Enabled() || err == nil || !c.sampled() {
		return ""
	}

	frames := framesFrom(callers)
	if len(frames) == 0 {
		// No stack: fall back to the caller's stack.
		frames = captureStack(1)
	}
	return c.captureError(c.scope, err, frames, modifiers)
}

// CaptureExceptionWithStackContext is CaptureExceptionWithStack with the scope
// ctx carries.
func (c *Client) CaptureExceptionWithStackContext(ctx context.Context, err error, callers []uintptr, modifiers ...EventModifier) string {
	if !c.Enabled() || err == nil || !c.sampled() {
		return ""
	}

	frames := framesFrom(callers)
	if len(frames) == 0 {
		frames = captureStack(1)
	}
	return c.captureError(c.scopeFor(ctx), err, frames, withTrace(ctx, modifiers))
}

// captureError builds and sends the event of an error.
func (c *Client) captureError(scope *Scope, err error, frames []Frame, modifiers []EventModifier) string {
	event := c.newEvent(scope, LevelError, errorType(err), err.Error(), frames)
	if event == nil {
		return ""
	}
	withErrorChain(event, err)
	return c.finish(event, modifiers...)
}

// CaptureMessage records a free-form message.
func (c *Client) CaptureMessage(level Level, message string, modifiers ...EventModifier) string {
	if !c.Enabled() || message == "" || !c.sampled() {
		return ""
	}

	event := c.newEvent(c.scope, level, "Message", message, captureStack(1))
	return c.finish(event, modifiers...)
}

// CaptureMessageContext records a free-form message with the scope ctx carries.
func (c *Client) CaptureMessageContext(ctx context.Context, level Level, message string, modifiers ...EventModifier) string {
	if !c.Enabled() || message == "" || !c.sampled() {
		return ""
	}

	event := c.newEvent(c.scopeFor(ctx), level, "Message", message, captureStack(1))
	return c.finish(event, withTrace(ctx, modifiers)...)
}

// CaptureRecovered turns a recover() value into a fatal event.
//
// skip: how many frames to drop while collecting the stack. Called straight from
// a defer, 1 is right.
func (c *Client) CaptureRecovered(recovered any, skip int, modifiers ...EventModifier) string {
	if !c.Enabled() || recovered == nil || !c.sampled() {
		return ""
	}
	return c.captureRecovered(c.scope, recovered, captureStack(skip+1), modifiers)
}

// CaptureRecoveredContext turns a recover() value into a fatal event with the
// scope ctx carries.
func (c *Client) CaptureRecoveredContext(ctx context.Context, recovered any, skip int, modifiers ...EventModifier) string {
	if !c.Enabled() || recovered == nil || !c.sampled() {
		return ""
	}
	return c.captureRecovered(c.scopeFor(ctx), recovered, captureStack(skip+1), withTrace(ctx, modifiers))
}

func (c *Client) captureRecovered(scope *Scope, recovered any, frames []Frame, modifiers []EventModifier) string {
	event := c.newEvent(scope, LevelFatal, "runtime.Error", "panic: "+describe(recovered), frames)
	if event == nil {
		return ""
	}
	if err, ok := recovered.(error); ok {
		withErrorChain(event, err)
	}
	return c.finish(event, modifiers...)
}

// CaptureEvent sends a ready-made event; for custom flows.
func (c *Client) CaptureEvent(event *Event) string {
	if !c.Enabled() || event == nil || !c.sampled() {
		return ""
	}
	if event.EventID == "" {
		event.EventID = newEventID()
	}
	return c.finish(event)
}

// withTrace puts the trace of the span ctx carries first, so a caller's own
// WithTraceID still wins.
func withTrace(ctx context.Context, modifiers []EventModifier) []EventModifier {
	trace := withTraceOf(ctx)
	if trace == nil {
		return modifiers
	}
	return append([]EventModifier{trace}, modifiers...)
}

// scopeFor picks the scope ctx carries, or the global one.
func (c *Client) scopeFor(ctx context.Context) *Scope {
	if scope := ScopeFromContext(ctx); scope != nil {
		return scope
	}
	return c.scope
}

// newEvent builds an event with the shared fields filled in, or returns nil when
// the same error was sent moments ago.
//
// The cheap part runs first: a repeat is recognised before any source file is
// read or the memory is measured.
func (c *Client) newEvent(scope *Scope, level Level, eventType, message string, frames []Frame) *Event {
	c.markInApp(frames)
	culprit := culpritOf(frames)

	repeats, send := c.dedupe.admit(eventType+"|"+culprit+"|"+message, time.Now())
	if !send {
		c.stats.duplicates.Add(1)
		return nil
	}

	c.source.addContext(frames, c.options.SourceContextLines)

	event := &Event{
		EventID:     newEventID(),
		Level:       level,
		Type:        eventType,
		Message:     message,
		Culprit:     culprit,
		Platform:    "go",
		Environment: c.options.Environment,
		Release:     c.options.Release,
		ServerName:  c.options.ServerName,
		Runtime:     runtime.Version(),
		OS:          runtime.GOOS + "/" + runtime.GOARCH,
		Stacktrace:  frames,
		Breadcrumbs: scope.breadcrumbs(),
		Tags:        scope.tags(map[string]any{"sdk": "bugfree-go/" + Version}),
		User:        scope.user(),
		Memory:      memoryStats(),
		OccurredAt:  timestamp(time.Now()),
	}
	if repeats > 0 {
		event.Extra = map[string]any{"repeats_dropped": repeats}
	}
	return event
}

// finish applies the modifiers and BeforeSend, and sends. It returns the id of the
// sent event, or an empty string when the event was dropped.
func (c *Client) finish(event *Event, modifiers ...EventModifier) string {
	if event == nil {
		return ""
	}
	for _, modify := range modifiers {
		if modify != nil {
			modify(event)
		}
	}

	if c.options.BeforeSend != nil {
		if event = c.options.BeforeSend(event); event == nil {
			c.stats.beforeSend.Add(1)
			return ""
		}
	}
	c.transport.Send(event)
	return event.EventID
}

// newEventID produces a random (version 4) UUID.
//
// The id is chosen here rather than by the server so the caller has it at once,
// and so a retried delivery is recognised as the same event.
func newEventID() string {
	var id [16]byte
	if _, err := cryptorand.Read(id[:]); err != nil {
		return ""
	}
	id[6] = id[6]&0x0f | 0x40
	id[8] = id[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", id[0:4], id[4:6], id[6:8], id[8:10], id[10:16])
}

// sampled reports whether the event is sent, according to the sample rate. It is
// asked before the event is built, so a dropped event costs nothing.
func (c *Client) sampled() bool {
	if c.options.SampleRate >= 1 {
		return true
	}
	c.randomMu.Lock()
	keep := c.random.Float64() <= c.options.SampleRate
	c.randomMu.Unlock()

	if !keep {
		c.stats.sampled.Add(1)
	}
	return keep
}

// Stats counts the events the client did not send, by reason.
type Stats struct {
	// Sampled were left out by SampleRate.
	Sampled uint64
	// Duplicates repeated an error sent within DedupeWindow.
	Duplicates uint64
	// BeforeSend were dropped by the BeforeSend hook.
	BeforeSend uint64
	// QueueFull arrived while the delivery queue was full.
	QueueFull uint64
	// RateLimited arrived while the server had asked to wait (a 429).
	RateLimited uint64
	// Transactions were dropped because their queue was full.
	Transactions uint64
}

// clientStats holds the counters behind Stats.
type clientStats struct {
	transactions atomic.Uint64
	sampled      atomic.Uint64
	duplicates   atomic.Uint64
	beforeSend   atomic.Uint64
}

// Stats reports how many events were not sent, and why. Monitoring that drops
// events silently looks like an application without errors.
func (c *Client) Stats() Stats {
	if c == nil {
		return Stats{}
	}
	stats := Stats{
		Sampled:      c.stats.sampled.Load(),
		Duplicates:   c.stats.duplicates.Load(),
		BeforeSend:   c.stats.beforeSend.Load(),
		Transactions: c.stats.transactions.Load(),
	}
	if transport, ok := c.transport.(*httpTransport); ok {
		stats.QueueFull = transport.droppedFull.Load()
		stats.RateLimited = transport.droppedPaused.Load()
	}
	return stats
}

// Flush sends the pending events.
func (c *Client) Flush(timeout time.Duration) bool {
	if !c.Enabled() {
		return true
	}
	return c.transport.Flush(timeout)
}

// culpritOf picks the first application frame as the error's owner.
func culpritOf(frames []Frame) string {
	for _, frame := range frames {
		if frame.InApp && frame.Function != "" {
			return frame.Function
		}
	}
	if len(frames) > 0 {
		return frames[0].Function
	}
	return ""
}

// memorySamples are the runtime metrics memoryStats reads.
//
// runtime/metrics is used rather than runtime.ReadMemStats, which stops the world:
// in an error storm every failing request would pause the whole program.
var memorySamples = []string{
	"/memory/classes/heap/objects:bytes",
	"/memory/classes/total:bytes",
	"/gc/cycles/total:gc-cycles",
	"/sched/goroutines:goroutines",
}

// memoryStats summarizes the memory state at the time of the event.
func memoryStats() map[string]any {
	samples := make([]metrics.Sample, len(memorySamples))
	for i, name := range memorySamples {
		samples[i].Name = name
	}
	metrics.Read(samples)

	value := func(i int) uint64 {
		if samples[i].Value.Kind() != metrics.KindUint64 {
			return 0
		}
		return samples[i].Value.Uint64()
	}
	return map[string]any{
		"heap_alloc_mb": value(0) / 1024 / 1024,
		"total_mb":      value(1) / 1024 / 1024,
		"num_gc":        value(2),
		"goroutines":    value(3),
	}
}

// --- Global shortcuts ---

// Current returns the global client (nil when Init was never called).
func Current() *Client {
	currentMu.RLock()
	defer currentMu.RUnlock()
	return current
}

// CaptureException records an error with the global client.
func CaptureException(err error, modifiers ...EventModifier) string {
	return Current().CaptureException(err, modifiers...)
}

// CaptureExceptionWithStackContext records an error with the global client, the
// stack recorded where it was created and the scope ctx carries.
func CaptureExceptionWithStackContext(ctx context.Context, err error, callers []uintptr, modifiers ...EventModifier) string {
	return Current().CaptureExceptionWithStackContext(ctx, err, callers, modifiers...)
}

// CaptureExceptionContext records an error with the global client and the scope
// ctx carries.
func CaptureExceptionContext(ctx context.Context, err error, modifiers ...EventModifier) string {
	return Current().CaptureExceptionContext(ctx, err, modifiers...)
}

// CaptureMessage records a message with the global client.
func CaptureMessage(level Level, message string, modifiers ...EventModifier) string {
	return Current().CaptureMessage(level, message, modifiers...)
}

// CaptureMessageContext records a message with the global client and the scope
// ctx carries.
func CaptureMessageContext(ctx context.Context, level Level, message string, modifiers ...EventModifier) string {
	return Current().CaptureMessageContext(ctx, level, message, modifiers...)
}

// AddBreadcrumb adds a step to the global scope.
func AddBreadcrumb(crumb Breadcrumb) {
	if client := Current(); client != nil {
		client.scope.AddBreadcrumb(crumb)
	}
}

// SetTag adds a tag to the global scope.
func SetTag(key string, value any) {
	if client := Current(); client != nil {
		client.scope.SetTag(key, value)
	}
}

// SetUser sets the user in the global scope.
//
// Every event of the process carries it, whichever request produced the event.
// Inside a server, set the user on the request's scope instead:
// ScopeFromContext(ctx).SetUser(...).
func SetUser(user User) {
	if client := Current(); client != nil {
		client.scope.SetUser(user)
	}
}

// Flush drains the global client's queue.
func Flush(timeout time.Duration) bool {
	return Current().Flush(timeout)
}

// Close shuts the global client down.
func Close() {
	if client := Current(); client != nil {
		client.Close()
	}
}
