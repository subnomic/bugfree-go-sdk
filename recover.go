package bugfree

import (
	"context"
	"log"
	"net/http"
	"net/url"
	"runtime/debug"
	"time"
)

// Recover records the panic and re-panics.
//
// Usage:
//
//	func worker() {
//	    defer bugfree.Recover()
//	    ...
//	}
//
// The panic is re-raised: monitoring must not change how the program behaves.
// Code that wants to swallow the panic uses RecoverAndContinue.
func Recover() {
	if recovered := recover(); recovered != nil {
		Current().CaptureRecovered(recovered, 2)
		// Leave a short window for the event to be sent: once the panic is
		// re-raised the process may end and the queue would not drain.
		Flush(2 * time.Second)
		panic(recovered)
	}
}

// RecoverAndContinue records the panic and swallows it.
//
// Use it when one request or job must not take the others down. The recovered
// value is returned; nil means there was no panic.
func RecoverAndContinue() any {
	recovered := recover()
	if recovered != nil {
		Current().CaptureRecovered(recovered, 2)
	}
	return recovered
}

// Go runs fn on a new goroutine with a scope of its own and records its panic.
//
// A deferred Recover only sees the panics of its own goroutine, so a goroutine
// started from a guarded request is not guarded at all: its panic ends the
// process without a trace in bugfree. The panic is recorded and swallowed, so
// one failing background task does not take the program down.
//
// The goroutine's scope inherits from the one ctx carries, so the request's user
// and tags go with its events.
func Go(ctx context.Context, fn func(ctx context.Context)) {
	Current().Go(ctx, fn)
}

// Go runs fn on a new goroutine with a scope of its own and records its panic.
func (c *Client) Go(ctx context.Context, fn func(ctx context.Context)) {
	scoped := c.ContextWithScope(ctx)
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				c.CaptureRecoveredContext(scoped, recovered, 1, WithTag("goroutine", "bugfree.Go"))
			}
		}()
		fn(scoped)
	}()
}

// Middleware wraps net/http handlers against panics.
//
// Each request gets a scope of its own in its context: handlers set the user and
// add breadcrumbs with ScopeFromContext(r.Context()) and capture with
// CaptureExceptionContext(r.Context(), err). The panic is recorded and logged,
// the client gets a 500, and the server stays up.
//
// http.ErrAbortHandler is net/http's way of aborting a response on purpose; it is
// neither recorded nor swallowed.
func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		client := Current()
		ctx := client.ContextWithScope(request.Context())
		request = request.WithContext(ctx)

		ctx, transaction := client.StartTransaction(ctx, request.Method+" "+request.URL.Path,
			WithOp("http.server"), ContinueTrace(request.Header.Get("traceparent")))
		request = request.WithContext(ctx)

		// The status is only needed for sessions and transactions; without them the
		// writer stays as it is.
		var status *statusWriter
		if client.TracksSessions() || transaction != nil {
			status = &statusWriter{ResponseWriter: writer}
			writer = status
		}

		defer func() {
			recovered := recover()
			if recovered == nil {
				if status != nil {
					client.RecordSession(sessionForStatus(status.status))
					transaction.SetHTTPStatus(max(status.status, http.StatusOK))
				}
				transaction.Finish()
				return
			}
			if recovered == http.ErrAbortHandler {
				transaction.SetStatus("cancelled")
				transaction.Finish()
				panic(recovered)
			}
			client.RecordSession(SessionCrashed)
			transaction.SetHTTPStatus(http.StatusInternalServerError)
			transaction.Finish()

			client.CaptureRecoveredContext(ctx, recovered, 2,
				WithRequest(Request{
					Method:    request.Method,
					URL:       RequestURL(request.URL),
					UserAgent: request.UserAgent(),
				}),
				WithTag("handler", "net/http"),
			)
			// Swallowing the panic takes it away from net/http's own log; without a
			// DSN the log line is all that is left of it.
			LogPanic(nil, recovered)
			http.Error(writer, "internal server error", http.StatusInternalServerError)
		}()

		next.ServeHTTP(writer, request)
	})
}

// RequestURL renders a request address for an event: without its query string,
// user info and fragment, which often carry tokens (signed links, reset links,
// ?key=). The server scrubs known keys, but only an address never sent is safe.
func RequestURL(address *url.URL) string {
	if address == nil {
		return ""
	}
	trimmed := *address
	trimmed.RawQuery = ""
	trimmed.ForceQuery = false
	trimmed.Fragment = ""
	trimmed.RawFragment = ""
	trimmed.User = nil
	return trimmed.String()
}

// LogPanic writes a recovered panic and the current stack to logger, the standard
// logger when nil. Middlewares that swallow a panic call it, so the panic is not
// lost where monitoring is off.
func LogPanic(logger *log.Logger, recovered any) {
	if logger == nil {
		logger = log.Default()
	}
	logger.Printf("panic recovered: %v\n%s", recovered, debug.Stack())
}
