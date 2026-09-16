package bugfree

import (
	"fmt"
	"net/http"
	"strings"
	"time"
)

// BreadcrumbTransport records every outgoing HTTP request as a breadcrumb.
//
// Usage:
//
//	client := &http.Client{Transport: bugfree.BreadcrumbTransport(nil)}
//
// A request made with a context carrying a scope (NewRequestWithContext) records
// on that scope, the global one otherwise. The address is kept without its query
// string, which often carries tokens; bodies and headers are never recorded.
func BreadcrumbTransport(base http.RoundTripper) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	return &breadcrumbTransport{base: base}
}

type breadcrumbTransport struct {
	base http.RoundTripper
}

// RoundTrip sends the request and records how it went.
func (t *breadcrumbTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	// The SDK's own deliveries are not steps of the application.
	if strings.HasPrefix(request.Header.Get("User-Agent"), "bugfree-go/") {
		return t.base.RoundTrip(request)
	}

	// Inside a transaction the call is a span, and the service called continues the
	// trace. RoundTrip must not change the caller's request, so a copy carries the header.
	_, span := StartSpan(request.Context(), "http.client", request.Method+" "+RequestURL(request.URL))
	if span != nil {
		request = request.Clone(request.Context())
		request.Header.Set("traceparent", span.Traceparent())
	}

	started := time.Now()
	response, err := t.base.RoundTrip(request)
	elapsed := time.Since(started).Round(time.Millisecond)

	if err != nil {
		span.SetStatus("unknown_error")
	} else {
		span.SetData("http.status_code", response.StatusCode)
		if response.StatusCode >= 400 {
			span.SetStatus("http_error")
		}
	}
	span.Finish()

	crumb := Breadcrumb{
		Category: "http",
		Message:  request.Method + " " + RequestURL(request.URL),
	}
	switch {
	case err != nil:
		crumb.Data = fmt.Sprintf("failed · %v", err)
		crumb.Level = "error"
	default:
		crumb.Data = fmt.Sprintf("%d · %s", response.StatusCode, elapsed)
		crumb.Level = levelForStatus(response.StatusCode)
	}

	scope := ScopeFromContext(request.Context())
	if scope == nil {
		scope = Current().Scope()
	}
	scope.AddBreadcrumb(crumb)
	return response, err
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
