package bugfree

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// defaultMaxBreadcrumbs is how many steps a scope keeps unless told otherwise.
const defaultMaxBreadcrumbs = 30

// Scope holds the shared context added to events: steps, tags and the user.
//
// It is safe for concurrent use. The client's own scope is global: every request
// of an HTTP server shares it, so a user set there is attached to the events of
// every other request too. Per-request context belongs in a scope of its own,
// created with ContextWithScope; the middlewares do that for you.
type Scope struct {
	mu     sync.RWMutex
	parent *Scope
	// global marks a client's own scope, whose breadcrumbs are not inherited.
	global bool
	crumbs []Breadcrumb
	limit  int
	tagMap map[string]any
	person *User
}

func newScope(limit int) *Scope {
	if limit <= 0 {
		limit = defaultMaxBreadcrumbs
	}
	return &Scope{
		crumbs: make([]Breadcrumb, 0, limit),
		limit:  limit,
		tagMap: make(map[string]any),
	}
}

// newGlobalScope builds a client's own scope.
func newGlobalScope(limit int) *Scope {
	scope := newScope(limit)
	scope.global = true
	return scope
}

// child returns an empty scope that inherits from s.
//
// Inheritance is live, not a copy: a tag set on the global scope after a request
// started still reaches that request's events. What is set on the child stays on
// the child.
//
// Tags and the user are inherited from any parent. Breadcrumbs only from a scope
// of the same flow (a goroutine started from a request inherits the request's
// steps): in a server the global scope's steps are those of every other request,
// other users' included, and would leak into this one's events.
func (s *Scope) child() *Scope {
	limit := defaultMaxBreadcrumbs
	if s != nil {
		limit = s.limit
	}
	child := newScope(limit)
	child.parent = s
	return child
}

// scopeKey is the context key a scope is stored under.
type scopeKey struct{}

// ContextWithScope returns a copy of ctx carrying a scope of its own.
//
// The new scope inherits from the scope ctx already carries, or from the client's
// global scope (its tags and user, not its breadcrumbs). Breadcrumbs, tags and the
// user set on it only reach the events captured with that context
// (CaptureExceptionContext and the like), so concurrent requests see neither each
// other's user nor each other's steps.
func (c *Client) ContextWithScope(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	parent := ScopeFromContext(ctx)
	if parent == nil {
		parent = c.Scope()
	}
	return context.WithValue(ctx, scopeKey{}, parent.child())
}

// ContextWithScope returns a copy of ctx with a scope of its own, inheriting from
// the global client.
func ContextWithScope(ctx context.Context) context.Context {
	return Current().ContextWithScope(ctx)
}

// ScopeFromContext returns the scope ctx carries, or nil when it carries none.
//
// A nil scope is safe to use: its methods do nothing.
func ScopeFromContext(ctx context.Context) *Scope {
	if ctx == nil {
		return nil
	}
	scope, _ := ctx.Value(scopeKey{}).(*Scope)
	return scope
}

// AddBreadcrumb adds a step; past the limit the oldest one is dropped.
func (s *Scope) AddBreadcrumb(crumb Breadcrumb) {
	if s == nil {
		return
	}
	if crumb.At == "" {
		crumb.At = timestamp(time.Now())
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.crumbs = append(s.crumbs, crumb)
	if len(s.crumbs) > s.limit {
		s.crumbs = s.crumbs[len(s.crumbs)-s.limit:]
	}
}

// SetTag adds or updates a tag.
func (s *Scope) SetTag(key string, value any) {
	if s == nil || key == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tagMap[key] = value
}

// SetUser sets the user context.
func (s *Scope) SetUser(user User) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.person = &user
}

// Clear resets the scope. What it inherits from its parent is left alone.
func (s *Scope) Clear() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.crumbs = s.crumbs[:0]
	s.tagMap = make(map[string]any)
	s.person = nil
}

// breadcrumbs copies the steps ordered newest to oldest.
//
// The interface shows the most recent step at the top; a copy is handed out so a
// scope change during sending cannot corrupt the event. A child scope interleaves
// its own steps with its parent's by time and keeps the newest ones; the global
// scope's steps are not inherited (see child).
func (s *Scope) breadcrumbs() []Breadcrumb {
	if s == nil {
		return nil
	}

	s.mu.RLock()
	own := make([]Breadcrumb, len(s.crumbs))
	for i, crumb := range s.crumbs {
		own[len(s.crumbs)-1-i] = crumb
	}
	s.mu.RUnlock()

	// The parent is read after the lock is released: locks are never held across
	// two scopes, so no order between them can deadlock.
	var inherited []Breadcrumb
	if s.parent != nil && !s.parent.global {
		inherited = s.parent.breadcrumbs()
	}
	if len(inherited) == 0 {
		if len(own) == 0 {
			return nil
		}
		return own
	}

	merged := append(own, inherited...)
	sort.SliceStable(merged, func(i, j int) bool {
		return breadcrumbTime(merged[i]).After(breadcrumbTime(merged[j]))
	})
	if len(merged) > s.limit {
		merged = merged[:s.limit]
	}
	return merged
}

// breadcrumbTime parses a step's time; an unreadable one sorts as the oldest.
func breadcrumbTime(crumb Breadcrumb) time.Time {
	at, err := time.Parse(time.RFC3339Nano, crumb.At)
	if err != nil {
		return time.Time{}
	}
	return at
}

// tags merges the scope tags with the extra tags given.
//
// The scope wins over extra, and a child wins over its parent.
func (s *Scope) tags(extra map[string]any) map[string]any {
	if s == nil {
		merged := make(map[string]any, len(extra))
		for key, value := range extra {
			merged[key] = value
		}
		return merged
	}

	merged := s.parent.tags(extra)
	s.mu.RLock()
	defer s.mu.RUnlock()
	for key, value := range s.tagMap {
		merged[key] = value
	}
	return merged
}

// user returns a copy of the user context, falling back to the parent's.
func (s *Scope) user() *User {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	person := s.person
	s.mu.RUnlock()

	if person == nil {
		return s.parent.user()
	}
	copied := *person
	return &copied
}

// EventModifier changes a single event before it is sent.
type EventModifier func(*Event)

// WithLevel changes the event's level.
func WithLevel(level Level) EventModifier {
	return func(event *Event) { event.Level = level }
}

// WithRequest attaches the request context.
func WithRequest(request Request) EventModifier {
	return func(event *Event) { event.Request = &request }
}

// WithUser attaches the user context.
func WithUser(user User) EventModifier {
	return func(event *Event) { event.User = &user }
}

// WithTag attaches a single tag.
func WithTag(key string, value any) EventModifier {
	return func(event *Event) {
		if event.Tags == nil {
			event.Tags = map[string]any{}
		}
		event.Tags[key] = value
	}
}

// WithExtra adds an extra field.
func WithExtra(key string, value any) EventModifier {
	return func(event *Event) {
		if event.Extra == nil {
			event.Extra = map[string]any{}
		}
		event.Extra[key] = value
	}
}

// WithTraceID ties the event to a request id.
func WithTraceID(traceID string) EventModifier {
	return func(event *Event) { event.TraceID = traceID }
}

// WithType sets the event type (the error class) explicitly.
//
// For wrapped errors "%T" usually gives the wrapper's type
// (*httpx.ServiceError and the like); the caller can pass its own label to show
// a readable name in the interface.
func WithType(name string) EventModifier {
	return func(event *Event) {
		if name != "" {
			event.Type = name
		}
	}
}

// WithFingerprint sets the grouping key explicitly.
func WithFingerprint(fingerprint string) EventModifier {
	return func(event *Event) { event.Fingerprint = fingerprint }
}

// errorType turns an error type into a readable name.
//
// The wrapper that only records a stack says nothing about the failure; the
// type of the error it wraps is named instead.
func errorType(err error) string {
	for {
		wrapper, ok := err.(*stackError)
		if !ok {
			return fmt.Sprintf("%T", err)
		}
		err = wrapper.err
	}
}

// describe turns a recover() value into text.
func describe(recovered any) string {
	switch typed := recovered.(type) {
	case string:
		return typed
	case error:
		return typed.Error()
	default:
		return fmt.Sprintf("%v", typed)
	}
}

// maxChainLength bounds how many wrapped errors an event lists.
const maxChainLength = 10

// errorChain lists the errors wrapped inside err, outermost first.
//
// The reported error's own type and message are already the event's; the chain
// holds what it wraps, through errors.Unwrap and errors.Join alike. It answers
// what "could not update the settings" was caused by.
func errorChain(err error) []map[string]string {
	var chain []map[string]string

	var walk func(current error, depth int)
	walk = func(current error, depth int) {
		if current == nil || len(chain) >= maxChainLength {
			return
		}
		// A stack recorder is not a link of the chain: it repeats what it wraps.
		if _, recorder := current.(*stackError); recorder {
			walk(errors.Unwrap(current), depth)
			return
		}
		if depth > 0 {
			chain = append(chain, map[string]string{
				"type":    errorType(current),
				"message": current.Error(),
			})
		}

		switch wrapper := current.(type) {
		case interface{ Unwrap() []error }:
			for _, inner := range wrapper.Unwrap() {
				walk(inner, depth+1)
			}
		default:
			walk(errors.Unwrap(current), depth+1)
		}
	}

	walk(err, 0)
	return chain
}

// withErrorChain attaches the wrapped errors to the event, when there are any.
func withErrorChain(event *Event, err error) {
	chain := errorChain(err)
	if len(chain) == 0 {
		return
	}
	if event.Extra == nil {
		event.Extra = map[string]any{}
	}
	event.Extra["error_chain"] = chain
}
