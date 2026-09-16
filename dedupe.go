package bugfree

import (
	"sync"
	"time"
)

// defaultDedupeWindow is how long a repeated error is held back unless told otherwise.
const defaultDedupeWindow = time.Second

// maxDedupeEntries bounds the errors remembered at once.
const maxDedupeEntries = 1000

// dedupe lets the same error through once per window.
//
// A failing endpoint under load produces one identical error per request. Sent
// one by one they fill the delivery queue, and the queue then drops whatever
// comes next, the rare errors worth seeing included. The repeats held back are
// counted and reported on the next event of the same error.
type dedupe struct {
	window time.Duration

	mu      sync.Mutex
	entries map[string]*dedupeEntry
}

type dedupeEntry struct {
	sentAt  time.Time
	dropped int
}

// newDedupe builds the filter; a negative window turns it off.
func newDedupe(window time.Duration) *dedupe {
	if window == 0 {
		window = defaultDedupeWindow
	}
	return &dedupe{window: window, entries: map[string]*dedupeEntry{}}
}

// admit decides whether the error with this signature is sent now. When it is,
// repeats is how many copies were held back since the last one was sent.
func (d *dedupe) admit(signature string, now time.Time) (repeats int, send bool) {
	if d == nil || d.window < 0 {
		return 0, true
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	entry, seen := d.entries[signature]
	if seen && now.Sub(entry.sentAt) < d.window {
		entry.dropped++
		return 0, false
	}

	if len(d.entries) >= maxDedupeEntries {
		d.evict(now)
	}
	if seen {
		repeats = entry.dropped
	}
	d.entries[signature] = &dedupeEntry{sentAt: now}
	return repeats, true
}

// evict forgets the errors whose window is over; when none is, the whole table
// goes, which at worst lets one repeat of each through.
func (d *dedupe) evict(now time.Time) {
	for signature, entry := range d.entries {
		if now.Sub(entry.sentAt) >= d.window {
			delete(d.entries, signature)
		}
	}
	if len(d.entries) >= maxDedupeEntries {
		d.entries = map[string]*dedupeEntry{}
	}
}
