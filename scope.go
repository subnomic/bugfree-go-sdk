package bugfree

import (
	"fmt"
	"sync"
	"time"
)

// Scope holds the shared context added to events: steps, tags and the user.
//
// It is safe for concurrent use; in HTTP servers more than one request shares
// the same scope.
type Scope struct {
	mu     sync.RWMutex
	crumbs []Breadcrumb
	limit  int
	tagMap map[string]any
	person *User
}

func newScope(limit int) *Scope {
	return &Scope{
		crumbs: make([]Breadcrumb, 0, limit),
		limit:  limit,
		tagMap: make(map[string]any),
	}
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

// Clear resets the scope.
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
// scope change during sending cannot corrupt the event.
func (s *Scope) breadcrumbs() []Breadcrumb {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.crumbs) == 0 {
		return nil
	}
	reversed := make([]Breadcrumb, len(s.crumbs))
	for i, crumb := range s.crumbs {
		reversed[len(s.crumbs)-1-i] = crumb
	}
	return reversed
}

// tags merges the scope tags with the extra tags given.
func (s *Scope) tags(extra map[string]any) map[string]any {
	merged := make(map[string]any, len(extra)+4)
	for key, value := range extra {
		merged[key] = value
	}
	if s == nil {
		return merged
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	for key, value := range s.tagMap {
		merged[key] = value
	}
	return merged
}

// user returns a copy of the user context.
func (s *Scope) user() *User {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.person == nil {
		return nil
	}
	copied := *s.person
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
func errorType(err error) string {
	return fmt.Sprintf("%T", err)
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
