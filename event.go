// Package bugfree sends panics and errors from Go applications to bugfree.
//
// Usage:
//
//	bugfree.Init(bugfree.Options{
//	    DSN:         os.Getenv("BUGFREE_DSN"),
//	    Environment: "production",
//	    Release:     "payment-api@v3.14.2",
//	})
//	defer bugfree.Flush(2 * time.Second)
//
//	defer bugfree.Recover() // catches the panic, records it and re-panics
package bugfree

import "time"

// Level is the severity of an event.
type Level string

// The supported levels.
const (
	LevelFatal   Level = "fatal"
	LevelError   Level = "error"
	LevelWarning Level = "warning"
	LevelInfo    Level = "info"
)

// Frame is a single frame of a stack trace.
//
// The Context field is the source code around the failing line; the interface
// uses that list to show what is written on which line.
type Frame struct {
	Function string            `json:"function"`
	File     string            `json:"file"`
	Line     int               `json:"line"`
	InApp    bool              `json:"in_app"`
	Package  string            `json:"package,omitempty"`
	Context  []ContextLine     `json:"context,omitempty"`
	Vars     map[string]string `json:"vars,omitempty"`
}

// ContextLine is a single line of source code.
type ContextLine struct {
	Line   int    `json:"line"`
	Source string `json:"source"`
}

// Breadcrumb is a step that happened before the error.
type Breadcrumb struct {
	Category string `json:"category"`
	Message  string `json:"message"`
	Data     string `json:"data,omitempty"`
	Level    string `json:"level,omitempty"`
	At       string `json:"at,omitempty"`
}

// Request is the HTTP request context of the event.
type Request struct {
	Method     string `json:"method,omitempty"`
	URL        string `json:"url,omitempty"`
	StatusCode int    `json:"status_code,omitempty"`
	UserAgent  string `json:"user_agent,omitempty"`
	Body       string `json:"body,omitempty"`
}

// User is the user context of the event. Optional, for privacy.
type User struct {
	ID        string `json:"id,omitempty"`
	Email     string `json:"email,omitempty"`
	IPAddress string `json:"ip_address,omitempty"`
}

// Event is the body sent to the ingest endpoint.
// The field names match the bugfree ingest API one to one.
type Event struct {
	Level    Level  `json:"level"`
	Type     string `json:"type"`
	Message  string `json:"message"`
	Culprit  string `json:"culprit,omitempty"`
	Platform string `json:"platform"`

	Environment string `json:"environment,omitempty"`
	Release     string `json:"release,omitempty"`
	ServerName  string `json:"server_name,omitempty"`
	Runtime     string `json:"runtime,omitempty"`
	OS          string `json:"os,omitempty"`

	Request *Request `json:"request,omitempty"`
	User    *User    `json:"user,omitempty"`
	TraceID string   `json:"trace_id,omitempty"`

	Stacktrace  []Frame        `json:"stacktrace,omitempty"`
	Breadcrumbs []Breadcrumb   `json:"breadcrumbs,omitempty"`
	Tags        map[string]any `json:"tags,omitempty"`
	Extra       map[string]any `json:"extra,omitempty"`
	Memory      map[string]any `json:"memory,omitempty"`

	DurationMS  int64  `json:"duration_ms,omitempty"`
	Fingerprint string `json:"fingerprint,omitempty"`
	OccurredAt  string `json:"occurred_at,omitempty"`
}

// ingestResponse is the server's ingest response.
type ingestResponse struct {
	EventID string `json:"event_id"`
	IssueID string `json:"issue_id"`
	ShortID string `json:"short_id"`
	IsNew   bool   `json:"is_new"`
	Sampled bool   `json:"sampled"`
}

// timestamp renders the event time in the format the API expects.
func timestamp(at time.Time) string {
	return at.UTC().Format(time.RFC3339Nano)
}
