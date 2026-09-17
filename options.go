package bugfree

import (
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Options is the client configuration.
type Options struct {
	// DSN carries the project key and the server address:
	//   http://<public_key>@host:3000/ingest
	// When empty the SDK stays silently disabled, so application code runs
	// unchanged in environments where monitoring is not set up.
	DSN string

	Environment string
	Release     string
	ServerName  string

	// SampleRate is between 0 and 1. Zero or negative counts as 1, so the zero
	// Options send everything: to send nothing, leave DSN empty.
	SampleRate float64

	// DedupeWindow holds back an error identical (type, location and message) to
	// one sent less than this long ago; the next one sent reports how many were
	// held back. 0 uses the default (1s); a negative value sends every copy.
	DedupeWindow time.Duration

	// SourceContextLines is how many lines to show above and below the failing
	// line in each frame. 0 uses the default (5); a negative value reads no source
	// at all.
	SourceContextLines int

	// SourceRoots maps the build path onto a local directory.
	//
	// In binaries built with -trimpath the frame paths look like
	// "github.com/bugfree/backend/internal/x.go", while the source files live
	// somewhere else inside the container:
	//   {"github.com/bugfree/backend/": "/app/src/backend/"}
	SourceRoots map[string]string

	// SourceFS reads the source from an embedded FS instead of the file system.
	// For installs that do not want to ship source next to the binary.
	SourceFS fs.FS

	// InAppPrefixes marks the application code. When empty nothing can be inferred
	// from the DSN, so only frames outside stdlib and the module cache count as
	// application code.
	InAppPrefixes []string

	// ProfilingInterval turns continuous profiling on: every interval a CPU profile
	// of ProfileDuration and a heap profile are taken and sent. 0 leaves it off.
	// A CPU profile costs a few percent of CPU while it runs.
	ProfilingInterval time.Duration

	// ProfileDuration is how long each CPU profile runs (10s by default).
	ProfileDuration time.Duration

	// TracesSampleRate is the share of traces recorded, between 0 and 1; 0 turns
	// tracing off. The decision is made from the trace id, so the services of one
	// trace agree without trusting each other.
	TracesSampleRate float64

	// TrustIncomingSampling follows the sampled flag of an incoming traceparent
	// header instead. Turn it on only when every caller is your own: anyone can
	// send the header and have all of their requests recorded.
	TrustIncomingSampling bool

	// TrackSessions counts every request the middlewares handle as a session, for
	// release health: the crash-free rate of each release. Counts are reported once
	// a minute, not per request.
	TrackSessions bool

	// MaxBreadcrumbs is how many steps to keep (30 by default).
	MaxBreadcrumbs int

	// FlushTimeout is how long a Flush call waits (2s by default).
	FlushTimeout time.Duration

	// ShutdownTimeout is how long Close waits for the queued events (5s by
	// default). After it, the request in flight is aborted and the rest dropped,
	// so an unreachable server cannot hold the application's shutdown hostage.
	ShutdownTimeout time.Duration

	// HTTPClient is an optional custom client.
	HTTPClient *http.Client

	// Transport can be replaced to intercept in tests.
	Transport Transport

	// Debug writes the SDK's own errors to stderr.
	Debug bool

	// BeforeSend is called before an event is sent; returning nil drops the event.
	// Use it to strip sensitive data.
	BeforeSend func(*Event) *Event
}

// dsn is a parsed DSN.
type dsn struct {
	publicKey string
	storeURL  string
	// base is the ingest address with the key: http://host/ingest/v1/<key>
	base string
}

// parseDSN parses the "http://key@host:3000/ingest" form.
//
// The trailing "/ingest" is optional and appended when missing, so
// "http://key@host:3000" is valid too.
func parseDSN(raw string) (*dsn, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, errors.New("bugfree: DSN is empty")
	}

	parsed, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("bugfree: DSN could not be parsed: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("bugfree: unsupported DSN scheme %q", parsed.Scheme)
	}
	if parsed.User == nil || parsed.User.Username() == "" {
		return nil, errors.New("bugfree: DSN is missing the public key (http://<key>@host/ingest)")
	}
	if parsed.Host == "" {
		return nil, errors.New("bugfree: DSN is missing the host")
	}

	key := parsed.User.Username()
	base := parsed.Scheme + "://" + parsed.Host

	path := strings.Trim(parsed.Path, "/")
	if path == "" {
		path = "ingest"
	}

	return &dsn{
		publicKey: key,
		storeURL:  fmt.Sprintf("%s/%s/v1/%s/store", base, path, key),
		base:      fmt.Sprintf("%s/%s/v1/%s", base, path, key),
	}, nil
}

// normalize fills the missing values with defaults.
func (o *Options) normalize() {
	if o.SampleRate <= 0 || o.SampleRate > 1 {
		o.SampleRate = 1
	}
	if o.SourceContextLines == 0 {
		o.SourceContextLines = 5
	}
	if o.MaxBreadcrumbs <= 0 {
		o.MaxBreadcrumbs = 30
	}
	if o.FlushTimeout <= 0 {
		o.FlushTimeout = 2 * time.Second
	}
	if o.ShutdownTimeout <= 0 {
		o.ShutdownTimeout = defaultCloseTimeout
	}
	if o.Environment == "" {
		o.Environment = "development"
	}
}
