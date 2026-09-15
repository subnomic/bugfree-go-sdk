package bugfree

import (
	"log"
	"math/rand"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"
)

// Version is the SDK version; used in the User-Agent and the event tags.
const Version = "0.1.1"

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
		scope:   newScope(options.MaxBreadcrumbs),
		source:  newSourceReader(options.SourceRoots, options.SourceFS),
		random:  rand.New(rand.NewSource(time.Now().UnixNano())),
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

// Close shuts the transport down.
func (c *Client) Close() {
	if c != nil && c.transport != nil {
		c.transport.Close()
	}
}

// CaptureException records an error.
func (c *Client) CaptureException(err error, modifiers ...EventModifier) {
	if !c.Enabled() || err == nil {
		return
	}

	event := c.newEvent(LevelError, errorType(err), err.Error(), captureStack(1))
	c.finish(event, modifiers...)
}

// CaptureExceptionWithStack records an error with the call stack captured where
// it was created.
//
// For returned (not thrown) errors the stack is meaningful where the error was
// created, not where it was handled: what you look for behind "the settings
// could not be updated" is the service function that produced it, not the HTTP
// layer that reported it. callers is collected with runtime.Callers at the point
// the error was created.
func (c *Client) CaptureExceptionWithStack(err error, callers []uintptr, modifiers ...EventModifier) {
	if !c.Enabled() || err == nil {
		return
	}

	frames := framesFrom(callers)
	if len(frames) == 0 {
		// No stack: fall back to the caller's stack.
		frames = captureStack(1)
	}

	event := c.newEvent(LevelError, errorType(err), err.Error(), frames)
	c.finish(event, modifiers...)
}

// CaptureMessage records a free-form message.
func (c *Client) CaptureMessage(level Level, message string, modifiers ...EventModifier) {
	if !c.Enabled() || message == "" {
		return
	}

	event := c.newEvent(level, "Message", message, captureStack(1))
	c.finish(event, modifiers...)
}

// CaptureRecovered turns a recover() value into a fatal event.
//
// skip: how many frames to drop while collecting the stack. Called straight from
// a defer, 1 is right.
func (c *Client) CaptureRecovered(recovered any, skip int, modifiers ...EventModifier) {
	if !c.Enabled() || recovered == nil {
		return
	}

	event := c.newEvent(LevelFatal, "runtime.Error", "panic: "+describe(recovered), captureStack(skip+1))
	c.finish(event, modifiers...)
}

// CaptureEvent sends a ready-made event; for custom flows.
func (c *Client) CaptureEvent(event *Event) {
	if !c.Enabled() || event == nil {
		return
	}
	c.finish(event)
}

// newEvent builds an event with the shared fields filled in.
func (c *Client) newEvent(level Level, eventType, message string, frames []Frame) *Event {
	c.markInApp(frames)
	c.source.addContext(frames, c.options.SourceContextLines)

	return &Event{
		Level:       level,
		Type:        eventType,
		Message:     message,
		Culprit:     culpritOf(frames),
		Platform:    "go",
		Environment: c.options.Environment,
		Release:     c.options.Release,
		ServerName:  c.options.ServerName,
		Runtime:     runtime.Version(),
		OS:          runtime.GOOS + "/" + runtime.GOARCH,
		Stacktrace:  frames,
		Breadcrumbs: c.scope.breadcrumbs(),
		Tags:        c.scope.tags(map[string]any{"sdk": "bugfree-go/" + Version}),
		User:        c.scope.user(),
		Memory:      memoryStats(),
		OccurredAt:  timestamp(time.Now()),
	}
}

// finish applies the modifiers, samples and sends.
func (c *Client) finish(event *Event, modifiers ...EventModifier) {
	for _, modify := range modifiers {
		if modify != nil {
			modify(event)
		}
	}

	if !c.sampled() {
		return
	}
	if c.options.BeforeSend != nil {
		if event = c.options.BeforeSend(event); event == nil {
			return
		}
	}
	c.transport.Send(event)
}

// sampled reports whether the event is sent, according to the sample rate.
func (c *Client) sampled() bool {
	if c.options.SampleRate >= 1 {
		return true
	}
	c.randomMu.Lock()
	defer c.randomMu.Unlock()
	return c.random.Float64() <= c.options.SampleRate
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

// memoryStats summarizes the memory state at the time of the event.
func memoryStats() map[string]any {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return map[string]any{
		"heap_alloc_mb": stats.HeapAlloc / 1024 / 1024,
		"heap_sys_mb":   stats.HeapSys / 1024 / 1024,
		"num_gc":        stats.NumGC,
		"goroutines":    runtime.NumGoroutine(),
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
func CaptureException(err error, modifiers ...EventModifier) {
	Current().CaptureException(err, modifiers...)
}

// CaptureMessage records a message with the global client.
func CaptureMessage(level Level, message string, modifiers ...EventModifier) {
	Current().CaptureMessage(level, message, modifiers...)
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
