package bugfree

import (
	"net/http"
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

// Middleware wraps net/http handlers against panics.
//
// The panic is caught and recorded, the client gets a 500, and the server stays up.
func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		defer func() {
			recovered := recover()
			if recovered == nil {
				return
			}

			Current().CaptureRecovered(recovered, 2,
				WithRequest(Request{
					Method:    request.Method,
					URL:       request.URL.String(),
					UserAgent: request.UserAgent(),
				}),
				WithTag("handler", "net/http"),
			)
			http.Error(writer, "internal server error", http.StatusInternalServerError)
		}()

		next.ServeHTTP(writer, request)
	})
}
