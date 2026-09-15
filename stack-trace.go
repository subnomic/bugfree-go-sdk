package bugfree

import (
	"runtime"
	"strings"
)

// maxFrames is the largest number of frames sent in one event.
const maxFrames = 40

// captureStack collects the frames of the call stack.
//
// runtime.CallersFrames is used instead of parsing the debug.Stack() text: the
// text format changes between Go releases, while this API is stable and gives
// the file/line/function directly.
//
// skip: how many frames to drop (0 = captureStack's caller).
func captureStack(skip int) []Frame {
	// +2: runtime.Callers and captureStack themselves are skipped.
	counters := make([]uintptr, maxFrames)
	count := runtime.Callers(skip+2, counters)
	if count == 0 {
		return nil
	}

	iterator := runtime.CallersFrames(counters[:count])
	frames := make([]Frame, 0, count)

	for {
		frame, more := iterator.Next()
		if frame.Function != "" || frame.File != "" {
			frames = append(frames, Frame{
				Function: frame.Function,
				File:     frame.File,
				Line:     frame.Line,
				Package:  packageOf(frame.Function),
			})
		}
		if !more {
			break
		}
	}

	return trimRuntimeFrames(frames)
}

// framesFrom turns already-collected program counters into frames.
//
// A stack captured with runtime.Callers is the call chain at the moment the
// error was created; this only resolves it.
func framesFrom(callers []uintptr) []Frame {
	if len(callers) == 0 {
		return nil
	}

	iterator := runtime.CallersFrames(callers)
	frames := make([]Frame, 0, len(callers))
	for {
		frame, more := iterator.Next()
		if frame.Function != "" || frame.File != "" {
			frames = append(frames, Frame{
				Function: frame.Function,
				File:     frame.File,
				Line:     frame.Line,
				Package:  packageOf(frame.Function),
			})
		}
		if !more {
			break
		}
	}
	return trimRuntimeFrames(frames)
}

// trimRuntimeFrames drops the frames of the panic machinery.
//
// After recover() the stack starts like this:
//
//	runtime.gopanic -> the real panic site -> ...
//
// When gopanic is found, everything before it (the SDK and recovery frames) is dropped.
func trimRuntimeFrames(frames []Frame) []Frame {
	start := 0
	for i, frame := range frames {
		if frame.Function == "runtime.gopanic" || frame.Function == "panic" {
			start = i + 1
			break
		}
	}

	trimmed := make([]Frame, 0, len(frames)-start)
	for _, frame := range frames[start:] {
		if isNoise(frame) {
			continue
		}
		trimmed = append(trimmed, frame)
	}
	return trimmed
}

// isNoise drops only the frames of the panic/runtime machinery.
//
// The SDK's own frames are not filtered here: captureStack collects from its
// caller onwards, so they never enter the stack anyway. Filtering by package
// prefix would also delete the frames of code running in the same package as the
// SDK (its own tests, for example).
func isNoise(frame Frame) bool {
	return strings.HasPrefix(frame.Function, "runtime.") ||
		strings.HasPrefix(frame.Function, "runtime/debug.")
}

// packageOf extracts the package path from a fully qualified function name.
//
// Example: "github.com/acme/pay/handler.(*Processor).Charge" -> "github.com/acme/pay/handler"
func packageOf(function string) string {
	if function == "" {
		return ""
	}
	// The package path ends at the first "." after the last "/".
	slash := strings.LastIndex(function, "/")
	dot := strings.Index(function[slash+1:], ".")
	if dot < 0 {
		return function
	}
	return function[:slash+1+dot]
}

// markInApp decides for each frame whether it belongs to the application.
func (c *Client) markInApp(frames []Frame) {
	for i := range frames {
		frames[i].InApp = c.isInApp(frames[i])
	}
}

// isInApp decides whether a frame is application code.
//
// The explicitly given prefixes are tried first; without them, everything outside
// the standard library and the module cache (dependencies) is application code.
func (c *Client) isInApp(frame Frame) bool {
	if len(c.options.InAppPrefixes) > 0 {
		for _, prefix := range c.options.InAppPrefixes {
			if prefix == "" {
				continue
			}
			if strings.HasPrefix(frame.Function, prefix) || strings.Contains(frame.File, prefix) {
				return true
			}
		}
		return false
	}

	file := frame.File
	switch {
	case file == "":
		return false
	case strings.Contains(file, "/pkg/mod/"), // downloaded dependencies
		strings.Contains(file, "/go/src/runtime/"),
		strings.Contains(file, "/libexec/src/"), // homebrew stdlib
		strings.Contains(file, "/usr/local/go/src/"),
		strings.HasPrefix(frame.Function, "runtime."):
		return false
	}
	return true
}
