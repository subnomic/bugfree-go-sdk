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
	"net/http"

	"github.com/gin-gonic/gin"
	bugfree "github.com/subnomic/bugfree-go-sdk"
)

// Options is the middleware behaviour.
type Options struct {
	// Repanic re-raises the panic after it is recorded.
	// For installs that have a recovery layer of their own.
	Repanic bool

	// SendRequestBody attaches the request body to the event. Off by default,
	// because a body may carry sensitive data.
	SendRequestBody bool

	// UserResolver extracts the user details from the request context.
	UserResolver func(*gin.Context) bugfree.User

	// OnPanic writes the response after a panic. Without it a plain 500 is
	// returned. Use it to keep the application's own JSON error shape.
	OnPanic func(*gin.Context, any)
}

// Recovery catches panics, records them in bugfree and writes the response.
func Recovery(options Options) gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			recovered := recover()
			if recovered == nil {
				return
			}

			capture(c, recovered, options)

			if options.Repanic {
				panic(recovered)
			}
			if options.OnPanic != nil {
				options.OnPanic(c, recovered)
				return
			}
			c.AbortWithStatus(http.StatusInternalServerError)
		}()

		c.Next()
	}
}

// capture sends the panic together with the request context.
func capture(c *gin.Context, recovered any, options Options) {
	modifiers := []bugfree.EventModifier{
		bugfree.WithRequest(requestOf(c, options.SendRequestBody)),
		bugfree.WithTag("handler", "gin"),
		bugfree.WithTag("route", c.FullPath()),
	}

	if requestID := requestIDOf(c); requestID != "" {
		modifiers = append(modifiers, bugfree.WithTraceID(requestID))
	}
	if options.UserResolver != nil {
		user := options.UserResolver(c)
		user.IPAddress = c.ClientIP()
		modifiers = append(modifiers, bugfree.WithUser(user))
	}

	// skip=2: this function and the deferred closure are skipped.
	bugfree.Current().CaptureRecovered(recovered, 2, modifiers...)
}

// requestOf extracts the request context.
func requestOf(c *gin.Context, includeBody bool) bugfree.Request {
	request := bugfree.Request{
		Method:    c.Request.Method,
		URL:       c.Request.URL.String(),
		UserAgent: c.Request.UserAgent(),
	}
	if includeBody {
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

// Breadcrumbs records every request as a step.
//
// When an error happens it answers "which requests came before".
func Breadcrumbs() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Next()

		bugfree.AddBreadcrumb(bugfree.Breadcrumb{
			Category: "http",
			Message:  c.Request.Method + " " + c.Request.URL.Path,
			Data:     http.StatusText(c.Writer.Status()),
			Level:    levelForStatus(c.Writer.Status()),
		})
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
