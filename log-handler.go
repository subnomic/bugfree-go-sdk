package bugfree

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"slices"
	"strings"
)

// LogHandlerOptions decides which log records the handler turns into what.
type LogHandlerOptions struct {
	// EventLevel is the lowest level recorded as an event; slog.LevelError when nil.
	EventLevel slog.Leveler

	// BreadcrumbLevel is the lowest level recorded as a breadcrumb; slog.LevelInfo
	// when nil.
	BreadcrumbLevel slog.Leveler

	// Client reports the records; the global client when nil.
	Client *Client
}

// LogHandler passes every record on to another slog handler and reports the
// important ones to bugfree: errors become events, the rest breadcrumbs.
//
// Usage:
//
//	logger := slog.New(bugfree.NewLogHandler(slog.NewJSONHandler(os.Stdout, nil), bugfree.LogHandlerOptions{}))
//	logger.ErrorContext(ctx, "charge failed", "order", id, "err", err)
//
// With the *Context logging methods, a record lands on the scope ctx carries.
// An attribute holding an error gives the event that error's type and chain.
type LogHandler struct {
	next    slog.Handler
	options LogHandlerOptions

	// attrs and group are what WithAttrs and WithGroup added, already prefixed.
	attrs []slog.Attr
	group string
}

// NewLogHandler wraps next. A nil next only reports and writes nothing.
func NewLogHandler(next slog.Handler, options LogHandlerOptions) *LogHandler {
	if options.EventLevel == nil {
		options.EventLevel = slog.LevelError
	}
	if options.BreadcrumbLevel == nil {
		options.BreadcrumbLevel = slog.LevelInfo
	}
	return &LogHandler{next: next, options: options}
}

// Enabled reports whether either the wrapped handler or bugfree wants the level.
func (h *LogHandler) Enabled(ctx context.Context, level slog.Level) bool {
	if level >= h.options.BreadcrumbLevel.Level() || level >= h.options.EventLevel.Level() {
		return true
	}
	return h.next != nil && h.next.Enabled(ctx, level)
}

// Handle reports the record, then hands it to the wrapped handler.
func (h *LogHandler) Handle(ctx context.Context, record slog.Record) error {
	client := h.options.Client
	if client == nil {
		client = Current()
	}

	if client.Enabled() {
		switch {
		case record.Level >= h.options.EventLevel.Level():
			if client.sampled() {
				h.captureEvent(ctx, client, record)
			}
		case record.Level >= h.options.BreadcrumbLevel.Level():
			client.scopeFor(ctx).AddBreadcrumb(Breadcrumb{
				Category: "log",
				Message:  record.Message,
				Data:     h.describeAttrs(record),
				Level:    breadcrumbLevel(record.Level),
			})
		}
	}

	if h.next != nil && h.next.Enabled(ctx, record.Level) {
		return h.next.Handle(ctx, record)
	}
	return nil
}

// WithAttrs returns a handler whose records carry attrs as well.
func (h *LogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	clone := *h
	clone.attrs = append(append([]slog.Attr(nil), h.attrs...), prefixAttrs(h.group, attrs)...)
	if h.next != nil {
		clone.next = h.next.WithAttrs(attrs)
	}
	return &clone
}

// WithGroup returns a handler that nests the attributes that follow under name.
func (h *LogHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	clone := *h
	clone.group = joinKey(h.group, name)
	if h.next != nil {
		clone.next = h.next.WithGroup(name)
	}
	return &clone
}

// captureEvent turns a record into an event.
func (h *LogHandler) captureEvent(ctx context.Context, client *Client, record slog.Record) {
	fields := h.fields(record)

	eventType, message := "log", record.Message
	var reported error
	for _, value := range fields {
		if err, ok := value.(error); ok && err != nil {
			reported = err
			eventType = errorType(err)
			message = record.Message + ": " + err.Error()
			break
		}
	}

	frames := errorFrames(reported)
	if frames == nil {
		frames = logStack(record.PC)
	}
	event := client.newEvent(client.scopeFor(ctx), eventLevel(record.Level), eventType, message, frames)
	if event == nil {
		return
	}
	if reported != nil {
		withErrorChain(event, reported)
	}
	if len(fields) > 0 {
		if event.Extra == nil {
			event.Extra = map[string]any{}
		}
		for key, value := range fields {
			if err, ok := value.(error); ok {
				value = err.Error()
			}
			event.Extra[key] = value
		}
	}
	event.Tags["logger"] = "slog"
	client.finish(event)
}

// fields flattens the handler's and the record's attributes into dotted keys.
func (h *LogHandler) fields(record slog.Record) map[string]any {
	fields := map[string]any{}
	for _, attr := range h.attrs {
		flattenAttr(fields, "", attr)
	}
	record.Attrs(func(attr slog.Attr) bool {
		flattenAttr(fields, h.group, attr)
		return true
	})
	return fields
}

// describeAttrs renders the attributes for a breadcrumb: key=value pairs.
func (h *LogHandler) describeAttrs(record slog.Record) string {
	fields := h.fields(record)
	if len(fields) == 0 {
		return ""
	}
	parts := make([]string, 0, len(fields))
	for key, value := range fields {
		parts = append(parts, fmt.Sprintf("%s=%v", key, value))
	}
	// Map order is random; a stable order keeps breadcrumbs comparable.
	slices.Sort(parts)
	data := strings.Join(parts, " ")
	if len(data) > maxBreadcrumbData {
		data = data[:maxBreadcrumbData] + "…"
	}
	return data
}

// maxBreadcrumbData bounds the text a log breadcrumb carries.
const maxBreadcrumbData = 500

func flattenAttr(fields map[string]any, prefix string, attr slog.Attr) {
	value := attr.Value.Resolve()
	key := joinKey(prefix, attr.Key)

	if value.Kind() == slog.KindGroup {
		for _, inner := range value.Group() {
			flattenAttr(fields, key, inner)
		}
		return
	}
	if attr.Key == "" {
		return
	}
	fields[key] = value.Any()
}

func prefixAttrs(group string, attrs []slog.Attr) []slog.Attr {
	if group == "" {
		return attrs
	}
	prefixed := make([]slog.Attr, len(attrs))
	for i, attr := range attrs {
		prefixed[i] = slog.Attr{Key: joinKey(group, attr.Key), Value: attr.Value}
	}
	return prefixed
}

func joinKey(prefix, key string) string {
	switch {
	case prefix == "":
		return key
	case key == "":
		return prefix
	default:
		return prefix + "." + key
	}
}

// logStack is the stack of the logging call, starting at the line that logged.
//
// The stack collected inside Handle starts in log/slog; the record's PC names the
// call site, and everything above it is dropped. Without a PC (a record built by
// hand) the whole stack is kept.
func logStack(pc uintptr) []Frame {
	frames := captureStack(2)
	if pc == 0 {
		return frames
	}

	site, _ := runtime.CallersFrames([]uintptr{pc}).Next()
	for i, frame := range frames {
		if frame.File == site.File && frame.Line == site.Line {
			return frames[i:]
		}
	}
	return frames
}

// eventLevel maps a slog level onto an event level.
func eventLevel(level slog.Level) Level {
	switch {
	case level > slog.LevelError:
		return LevelFatal
	case level >= slog.LevelError:
		return LevelError
	case level >= slog.LevelWarn:
		return LevelWarning
	default:
		return LevelInfo
	}
}

// breadcrumbLevel maps a slog level onto a breadcrumb level.
func breadcrumbLevel(level slog.Level) string {
	switch {
	case level >= slog.LevelError:
		return "error"
	case level >= slog.LevelWarn:
		return "warning"
	case level >= slog.LevelInfo:
		return "info"
	default:
		return "debug"
	}
}
