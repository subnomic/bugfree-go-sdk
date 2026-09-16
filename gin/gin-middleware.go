// Package bugfreegin joins the bugfree SDK with Gin.
//
// It is a separate package so the core SDK has no gin dependency; projects that
// do not use gin never pull this in.
//
// Usage:
//
//	router.Use(bugfreegin.Recovery(bugfreegin.Options{Repanic: false}))
package bugfreegin

import (
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
	bugfree "github.com/subnomic/bugfree-go-sdk"
)

// Options is the middleware behaviour.
type Options struct {
	// Repanic re-raises the panic after it is recorded.
	// For installs that have a recovery layer of their own.
	Repanic bool

	// Logger receives the panic and its stack when the panic is not re-raised;
	// the standard logger when nil. A swallowed panic is always logged: with an
	// empty DSN the log line is all that is left of it.
	Logger *log.Logger

	// SendRequestBody attaches the request body to the event. Off by default,
	// because a body may carry sensitive data.
	SendRequestBody bool

	// SendQueryString keeps the query string in the request address. Off by
	// default: tokens travel in query strings (signed links, reset links, ?key=).
	SendQueryString bool

	// UserResolver extracts the user details from the request context.
	UserResolver func(*gin.Context) bugfree.User

	// OmitClientIP leaves the client IP out of the user. By default it fills the
	// user's IPAddress when the resolver left it empty.
	OmitClientIP bool

	// RequestIDResolver returns the request id, sent as the event's trace id. By
	// default the "request_id" context key or the X-Request-Id header is used.
	RequestIDResolver func(*gin.Context) string

	// OnPanic writes the response after a panic. Without it a plain 500 is
	// returned. Use it to keep the application's own JSON error shape.
	OnPanic func(*gin.Context, any)

	// CaptureErrors also records the errors handlers attach with c.Error when the
	// response ends with a 5xx. Most failures in Go are returned errors, not
	// panics; without this they never reach bugfree. Leave it off when the
	// application reports its 5xx errors itself, or they are recorded twice.
	//
	// An error created with bugfree.WithStack or bugfree.Errorf is reported with
	// the stack of the place it was created.
	CaptureErrors bool
}

// Recovery catches panics, records them in bugfree and writes the response.
//
// Every request gets a scope of its own: set the user or add breadcrumbs with
// Scope(c), and capture with bugfree.CaptureExceptionContext(c.Request.Context(), err).
//
// http.ErrAbortHandler, net/http's way of aborting a response on purpose, is
// neither recorded nor swallowed.
func Recovery(options Options) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := bugfree.ContextWithScope(c.Request.Context())
		// The route is only known once gin matched it; the transaction is renamed then.
		ctx, transaction := bugfree.StartTransaction(ctx, c.Request.Method+" "+c.Request.URL.Path,
			bugfree.WithOp("http.server"), bugfree.ContinueTrace(c.GetHeader("traceparent")))
		c.Request = c.Request.WithContext(ctx)

		defer func() {
			recovered := recover()
			if route := c.FullPath(); route != "" {
				transaction.SetName(c.Request.Method + " " + route)
			}
			if recovered == nil {
				transaction.SetHTTPStatus(c.Writer.Status())
				transaction.Finish()
				return
			}
			if recovered == http.ErrAbortHandler {
				transaction.SetStatus("cancelled")
				transaction.Finish()
				panic(recovered)
			}

			transaction.SetHTTPStatus(http.StatusInternalServerError)
			transaction.Finish()
			bugfree.Current().RecordSession(bugfree.SessionCrashed)
			capture(c, recovered, options)

			if options.Repanic {
				panic(recovered)
			}
			bugfree.LogPanic(options.Logger, recovered)
			if options.OnPanic != nil {
				options.OnPanic(c, recovered)
				return
			}
			c.AbortWithStatus(http.StatusInternalServerError)
		}()

		c.Next()

		if c.Writer.Status() >= http.StatusInternalServerError {
			bugfree.Current().RecordSession(bugfree.SessionErrored)
		} else {
			bugfree.Current().RecordSession(bugfree.SessionOK)
		}
		if options.CaptureErrors {
			captureErrors(c, options)
		}
	}
}

// Scope returns the request's scope; nil (and safe to use) outside Recovery.
func Scope(c *gin.Context) *bugfree.Scope {
	return bugfree.ScopeFromContext(c.Request.Context())
}

// capture sends the panic together with the request context.
func capture(c *gin.Context, recovered any, options Options) {
	// skip=2: this function and the deferred closure are skipped.
	bugfree.Current().CaptureRecoveredContext(c.Request.Context(), recovered, 2, requestModifiers(c, options)...)
}

// captureErrors records the errors attached to a request that ended with a 5xx.
func captureErrors(c *gin.Context, options Options) {
	if len(c.Errors) == 0 || c.Writer.Status() < http.StatusInternalServerError {
		return
	}

	for _, attached := range c.Errors {
		recorded := bugfree.StackOf(attached.Err)
		modifiers := append(requestModifiers(c, options),
			func(event *bugfree.Event) {
				event.Request.StatusCode = c.Writer.Status()
				if recorded != nil {
					return
				}
				// The stack here is the middleware chain, the same for every error, and
				// would group unrelated errors together. Without it the server groups by
				// the error's type, the route and the message.
				event.Stacktrace = nil
				event.Culprit = c.Request.Method + " " + c.FullPath()
			},
		)
		bugfree.Current().CaptureExceptionContext(c.Request.Context(), attached.Err, modifiers...)
	}
}

// requestModifiers builds what every event of a request carries.
func requestModifiers(c *gin.Context, options Options) []bugfree.EventModifier {
	modifiers := []bugfree.EventModifier{
		bugfree.WithRequest(requestOf(c, options)),
		bugfree.WithTag("handler", "gin"),
		bugfree.WithTag("route", c.FullPath()),
	}

	requestID := ""
	if options.RequestIDResolver != nil {
		requestID = options.RequestIDResolver(c)
	} else {
		requestID = requestIDOf(c)
	}
	if requestID != "" {
		modifiers = append(modifiers, bugfree.WithTraceID(requestID))
	}

	if options.UserResolver != nil {
		user := options.UserResolver(c)
		if user.IPAddress == "" && !options.OmitClientIP {
			user.IPAddress = c.ClientIP()
		}
		modifiers = append(modifiers, bugfree.WithUser(user))
	}
	return modifiers
}

// requestOf extracts the request context.
func requestOf(c *gin.Context, options Options) bugfree.Request {
	address := bugfree.RequestURL(c.Request.URL)
	if options.SendQueryString {
		address = c.Request.URL.String()
	}

	request := bugfree.Request{
		Method:    c.Request.Method,
		URL:       address,
		UserAgent: c.Request.UserAgent(),
	}
	if options.SendRequestBody {
		// The body may have been read by the handler; the copy gin read is used
		// when there is one.
		if raw, ok := c.Get(gin.BodyBytesKey); ok {
			if body, ok := raw.([]byte); ok {
				request.Body = string(body)
			}
		}
	}
	return request
}

// requestIDOf tries the common request-id keys.
func requestIDOf(c *gin.Context) string {
	if value := c.GetString("request_id"); value != "" {
		return value
	}
	if value := c.GetHeader("X-Request-Id"); value != "" {
		return value
	}
	return ""
}

// Breadcrumbs records every request as a step on its own scope.
//
// Register it after Recovery, so the request has a scope: the step then reaches
// the events that request produces afterwards (CaptureErrors among them) and no
// other request's. Without a request scope it falls back to the global one.
func Breadcrumbs() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Next()

		crumb := bugfree.Breadcrumb{
			Category: "http",
			Message:  c.Request.Method + " " + c.Request.URL.Path,
			Data:     http.StatusText(c.Writer.Status()),
			Level:    levelForStatus(c.Writer.Status()),
		}
		if scope := Scope(c); scope != nil {
			scope.AddBreadcrumb(crumb)
			return
		}
		bugfree.AddBreadcrumb(crumb)
	}
}

// levelForStatus turns an HTTP status into a breadcrumb level.
func levelForStatus(status int) string {
	switch {
	case status >= 500:
		return "error"
	case status >= 400:
		return "warning"
	default:
		return "info"
	}
}
