package bugfree

import (
	"errors"
	"fmt"
	"runtime"
)

// stackError is an error that remembers where it was created.
type stackError struct {
	err     error
	callers []uintptr
}

func (e *stackError) Error() string      { return e.err.Error() }
func (e *stackError) Unwrap() error      { return e.err }
func (e *stackError) Callers() []uintptr { return e.callers }

// WithStack records the call stack at this point on err.
//
// Most Go failures are returned errors that reach a reporter far from where they
// happened; the reporter's own stack then points at the HTTP layer. The capture
// calls, and gin's CaptureErrors, use the stack recorded here instead. An error
// that already carries a stack is returned as it is, so wrapping twice keeps the
// deepest one.
//
//	if err != nil {
//	    return bugfree.WithStack(err)
//	}
func WithStack(err error) error {
	if err == nil {
		return nil
	}
	if errorCallers(err) != nil {
		return err
	}
	return &stackError{err: err, callers: callers(2)}
}

// Errorf is fmt.Errorf that records the call stack at this point.
func Errorf(format string, args ...any) error {
	return &stackError{err: fmt.Errorf(format, args...), callers: callers(2)}
}

// callers collects the program counters above skip (1 = callers' caller).
func callers(skip int) []uintptr {
	counters := make([]uintptr, maxFrames)
	count := runtime.Callers(skip+1, counters)
	return counters[:count]
}

// stackCarrier is any error that can say where it was created: this package's,
// or an application's own error type with the same method.
type stackCarrier interface {
	Callers() []uintptr
}

// errorCallers finds the deepest stack recorded in err's chain, or nil.
func errorCallers(err error) []uintptr {
	var found []uintptr
	walkErrors(err, func(current error) {
		if carrier, ok := current.(stackCarrier); ok {
			if stack := carrier.Callers(); len(stack) > 0 {
				found = stack
			}
		}
	})
	return found
}

// StackOf returns the stack recorded in err's chain (the deepest one), or nil when
// no error of the chain recorded one. Integrations use it to decide whether the
// capture site's stack is worth keeping.
func StackOf(err error) []uintptr {
	return errorCallers(err)
}

// errorFrames resolves the deepest recorded stack of err's chain, or nil.
func errorFrames(err error) []Frame {
	stack := errorCallers(err)
	if stack == nil {
		return nil
	}
	frames := framesFrom(stack)
	if len(frames) == 0 {
		return nil
	}
	return frames
}

// walkErrors visits err and everything it wraps, outermost first.
func walkErrors(err error, visit func(error)) {
	if err == nil {
		return
	}
	visit(err)
	switch wrapper := err.(type) {
	case interface{ Unwrap() []error }:
		for _, inner := range wrapper.Unwrap() {
			walkErrors(inner, visit)
		}
	default:
		walkErrors(errors.Unwrap(err), visit)
	}
}
