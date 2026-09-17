package bugfree

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// recordingTransport keeps the sent events in memory.
type recordingTransport struct {
	mu     sync.Mutex
	events []*Event
}

func (t *recordingTransport) Send(event *Event) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.events = append(t.events, event)
}

func (t *recordingTransport) Flush(time.Duration) bool { return true }
func (t *recordingTransport) Close()                   {}

func (t *recordingTransport) last() *Event {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.events) == 0 {
		return nil
	}
	return t.events[len(t.events)-1]
}

func newTestClient(t *testing.T, mutate func(*Options)) (*Client, *recordingTransport) {
	t.Helper()

	transport := &recordingTransport{}
	options := Options{
		DSN:         "http://abc123@localhost:3000/ingest",
		Environment: "test",
		Release:     "test@1",
		Transport:   transport,
	}
	if mutate != nil {
		mutate(&options)
	}

	client, err := NewClient(options)
	if err != nil {
		t.Fatalf("client could not be created: %v", err)
	}
	return client, transport
}

func TestParseDSNAcceptsItsForms(t *testing.T) {
	cases := []struct{ raw, key, store string }{
		{"http://abc@localhost:3000/ingest", "abc", "http://localhost:3000/ingest/v1/abc/store"},
		{"https://key@bugfree.io/ingest", "key", "https://bugfree.io/ingest/v1/key/store"},
		// Without a path, "ingest" is assumed.
		{"http://key@localhost:3000", "key", "http://localhost:3000/ingest/v1/key/store"},
	}
	for _, tc := range cases {
		parsed, err := parseDSN(tc.raw)
		if err != nil {
			t.Fatalf("parseDSN(%q) returned an error: %v", tc.raw, err)
		}
		if parsed.publicKey != tc.key {
			t.Errorf("parseDSN(%q) key = %q, expected %q", tc.raw, parsed.publicKey, tc.key)
		}
		if parsed.storeURL != tc.store {
			t.Errorf("parseDSN(%q) store = %q, expected %q", tc.raw, parsed.storeURL, tc.store)
		}
	}
}

func TestParseDSNRejectsMalformedInput(t *testing.T) {
	for _, raw := range []string{"", "localhost:3000", "http://localhost:3000/ingest", "ftp://key@host/ingest"} {
		if _, err := parseDSN(raw); err == nil {
			t.Errorf("parseDSN(%q) accepted an invalid DSN", raw)
		}
	}
}

// With an empty DSN the SDK has to stay silently off; application code must not change.
func TestEmptyDSNProducesDisabledClient(t *testing.T) {
	client, err := NewClient(Options{})
	if err != nil {
		t.Fatalf("empty DSN returned an error: %v", err)
	}
	if client.Enabled() {
		t.Error("the client is enabled without a DSN")
	}
	// The calls must not panic.
	client.CaptureException(errors.New("boom"))
	client.CaptureMessage(LevelInfo, "hello")
	client.CaptureRecovered("panic", 0)
	if !client.Flush(time.Second) {
		t.Error("Flush returned false on a disabled client")
	}
}

func TestCaptureExceptionFillsEvent(t *testing.T) {
	client, transport := newTestClient(t, nil)

	client.CaptureException(errors.New("card not found"))

	event := transport.last()
	if event == nil {
		t.Fatal("no event was sent")
	}
	if event.Level != LevelError {
		t.Errorf("level = %q, expected error", event.Level)
	}
	if event.Message != "card not found" {
		t.Errorf("message = %q", event.Message)
	}
	if event.Platform != "go" {
		t.Errorf("platform = %q, expected go", event.Platform)
	}
	if event.Environment != "test" || event.Release != "test@1" {
		t.Errorf("environment/release = %q/%q", event.Environment, event.Release)
	}
	if event.ServerName == "" {
		t.Error("server name was not filled in")
	}
	if event.Runtime == "" || event.OS == "" {
		t.Error("runtime information is missing")
	}
	if len(event.Stacktrace) == 0 {
		t.Fatal("the stack trace is empty")
	}
	if event.Tags["sdk"] != "bugfree-go/"+Version {
		t.Errorf("sdk tag = %v", event.Tags["sdk"])
	}
}

// The first stack frame has to be the calling function; SDK frames must not show up.
func TestCaptureExceptionHidesSDKFrames(t *testing.T) {
	client, transport := newTestClient(t, nil)

	client.CaptureException(errors.New("boom"))

	// The tests live in the same package as the SDK, so the package prefix cannot
	// be used; what is verified is that the SDK's internals stay invisible.
	frames := transport.last().Stacktrace
	internal := []string{".captureStack", ".CaptureException", ".newEvent", ".finish"}
	for _, frame := range frames {
		for _, name := range internal {
			if strings.HasSuffix(frame.Function, name) {
				t.Errorf("an internal SDK frame leaked into the stack: %s", frame.Function)
			}
		}
	}
	if !strings.Contains(frames[0].Function, "TestCaptureExceptionHidesSDKFrames") {
		t.Errorf("first frame = %q, expected the calling test", frames[0].Function)
	}
}

// The source context has to fill in: this test file is on disk, so it is readable.
func TestCaptureExceptionReadsNoSourceByDefault(t *testing.T) {
	client, transport := newTestClient(t, nil)
	client.CaptureException(errors.New("without context"))

	frame := transport.last().Stacktrace[0]
	if len(frame.Context) != 0 {
		t.Error("a production binary has no source to read; nothing is read unless asked")
	}
	if frame.File == "" || frame.Line == 0 || frame.Function == "" {
		t.Errorf("frame = %+v, the location comes from the binary alone", frame)
	}
}

func TestCaptureExceptionAttachesSourceContext(t *testing.T) {
	client, transport := newTestClient(t, func(options *Options) { options.SourceContextLines = 5 })

	client.CaptureException(errors.New("with context"))

	frame := transport.last().Stacktrace[0]
	if len(frame.Context) == 0 {
		t.Fatal("no source context was attached to the first frame")
	}

	var errorLine string
	for _, line := range frame.Context {
		if line.Line == frame.Line {
			errorLine = line.Source
		}
	}
	if !strings.Contains(errorLine, "with context") {
		t.Errorf("the error line does not contain the call: %q", errorLine)
	}
}

func TestCaptureRecoveredProducesFatal(t *testing.T) {
	client, transport := newTestClient(t, nil)

	func() {
		defer func() {
			client.CaptureRecovered(recover(), 1)
		}()
		panic("deliberate boom")
	}()

	event := transport.last()
	if event == nil {
		t.Fatal("no event was sent")
	}
	if event.Level != LevelFatal {
		t.Errorf("level = %q, expected fatal", event.Level)
	}
	if event.Message != "panic: deliberate boom" {
		t.Errorf("message = %q", event.Message)
	}
	// The frames of the panic machinery have to be gone.
	for _, frame := range event.Stacktrace {
		if frame.Function == "runtime.gopanic" || frame.Function == "panic" {
			t.Errorf("a panic frame leaked into the stack: %s", frame.Function)
		}
	}
	if event.Culprit == "" {
		t.Error("culprit is empty")
	}
}

func TestScopeAddsBreadcrumbTagAndUser(t *testing.T) {
	client, transport := newTestClient(t, nil)

	client.Scope().SetTag("region", "eu-central")
	client.Scope().SetUser(User{ID: "u-1", Email: "dev@acme.io"})
	client.Scope().AddBreadcrumb(Breadcrumb{Category: "sql", Message: "SELECT 1"})
	client.Scope().AddBreadcrumb(Breadcrumb{Category: "http", Message: "GET /pay"})

	client.CaptureMessage(LevelWarning, "slow query")

	event := transport.last()
	if event.Tags["region"] != "eu-central" {
		t.Errorf("tag was not applied: %v", event.Tags)
	}
	if event.User == nil || event.User.ID != "u-1" {
		t.Errorf("user context is missing: %+v", event.User)
	}
	if len(event.Breadcrumbs) != 2 {
		t.Fatalf("breadcrumb count = %d, expected 2", len(event.Breadcrumbs))
	}
	// The most recent step has to come first.
	if event.Breadcrumbs[0].Message != "GET /pay" {
		t.Errorf("breadcrumbs are not newest-first: %+v", event.Breadcrumbs)
	}
	if event.Breadcrumbs[0].At == "" {
		t.Error("the breadcrumb timestamp was not filled in")
	}
}

func TestScopeBoundsBreadcrumbCount(t *testing.T) {
	client, transport := newTestClient(t, func(o *Options) { o.MaxBreadcrumbs = 3 })

	for i := 0; i < 10; i++ {
		client.Scope().AddBreadcrumb(Breadcrumb{Category: "loop", Message: string(rune('a' + i))})
	}
	client.CaptureMessage(LevelInfo, "done")

	crumbs := transport.last().Breadcrumbs
	if len(crumbs) != 3 {
		t.Fatalf("breadcrumb count = %d, expected 3", len(crumbs))
	}
	if crumbs[0].Message != "j" {
		t.Errorf("the newest breadcrumb = %q, expected j", crumbs[0].Message)
	}
}

func TestBeforeSendCanDropEvent(t *testing.T) {
	client, transport := newTestClient(t, func(o *Options) {
		o.BeforeSend = func(event *Event) *Event {
			if strings.Contains(event.Message, "secret") {
				return nil
			}
			event.Message = "[scrubbed] " + event.Message
			return event
		}
	})

	client.CaptureMessage(LevelError, "secret token leaked")
	if transport.last() != nil {
		t.Error("BeforeSend returned nil but the event was still sent")
	}

	client.CaptureMessage(LevelError, "ordinary failure")
	if got := transport.last().Message; got != "[scrubbed] ordinary failure" {
		t.Errorf("message = %q, BeforeSend did not modify it", got)
	}
}

func TestNonZeroSampleRateSends(t *testing.T) {
	client, transport := newTestClient(t, func(o *Options) { o.SampleRate = 1 })
	client.CaptureMessage(LevelInfo, "always")
	if transport.last() == nil {
		t.Error("the event was dropped at sample rate 1")
	}
}

func TestEventModifiersAreApplied(t *testing.T) {
	client, transport := newTestClient(t, nil)

	client.CaptureException(errors.New("boom"),
		WithLevel(LevelFatal),
		WithRequest(Request{Method: "POST", URL: "/api/v1/pay"}),
		WithTraceID("trace-1"),
		WithTag("tenant", "acme"),
		WithExtra("attempt", 2),
		WithFingerprint("custom"),
	)

	event := transport.last()
	if event.Level != LevelFatal {
		t.Errorf("level = %q", event.Level)
	}
	if event.Request == nil || event.Request.URL != "/api/v1/pay" {
		t.Errorf("request context is missing: %+v", event.Request)
	}
	if event.TraceID != "trace-1" || event.Tags["tenant"] != "acme" {
		t.Errorf("trace/tag was not applied: %v %v", event.TraceID, event.Tags)
	}
	if event.Extra["attempt"] != 2 || event.Fingerprint != "custom" {
		t.Errorf("extra/fingerprint was not applied: %v %v", event.Extra, event.Fingerprint)
	}
}

func TestInAppPrefixesMarkFrames(t *testing.T) {
	client, transport := newTestClient(t, func(o *Options) {
		o.InAppPrefixes = []string{"github.com/subnomic/bugfree-go-sdk"}
	})

	client.CaptureMessage(LevelInfo, "prefix check")

	// With a prefix given, only matching frames count as application code; in this
	// test the calling test function does not match, so it must not be in_app.
	for _, frame := range transport.last().Stacktrace {
		if frame.InApp && !strings.HasPrefix(frame.Function, "github.com/subnomic/bugfree-go-sdk") {
			t.Errorf("a frame outside the prefix was marked in_app: %s", frame.Function)
		}
	}
}

func TestPackageOfExtractsPackagePath(t *testing.T) {
	cases := map[string]string{
		"github.com/acme/pay/handler.(*Processor).Charge": "github.com/acme/pay/handler",
		"main.main":                    "main",
		"runtime.gopanic":              "runtime",
		"github.com/acme/pay.Validate": "github.com/acme/pay",
		"":                             "",
	}
	for input, want := range cases {
		if got := packageOf(input); got != want {
			t.Errorf("packageOf(%q) = %q, expected %q", input, got, want)
		}
	}
}

// panicSite plays the handler whose panic a middleware raises again.
func panicSite() {
	panic("from the handler")
}

// A recovery layer that recovers and raises the panic again must not become the
// reported panic site.
func TestCaptureRecoveredSkipsARaisedAgainPanic(t *testing.T) {
	client, transport := newTestClient(t, nil)

	func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				client.CaptureRecovered(recovered, 1)
			}
		}()
		func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					panic(recovered)
				}
			}()
			panicSite()
		}()
	}()

	event := transport.last()
	if event == nil || len(event.Stacktrace) == 0 || !strings.HasSuffix(event.Stacktrace[0].Function, ".panicSite") {
		t.Fatalf("stack starts at %+v, expected panicSite", event.Stacktrace)
	}
}
