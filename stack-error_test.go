package bugfree

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// loadSettings plays the service function deep in the stack that fails.
func loadSettings() error {
	return WithStack(errors.New("settings row is missing"))
}

func TestCaptureUsesTheStackWhereTheErrorWasCreated(t *testing.T) {
	client, transport := newTestClient(t, nil)

	err := fmt.Errorf("update settings: %w", loadSettings())
	client.CaptureExceptionContext(context.Background(), err)

	event := transport.last()
	if len(event.Stacktrace) == 0 || !strings.HasSuffix(event.Stacktrace[0].Function, ".loadSettings") {
		t.Fatalf("stack starts at %+v, expected loadSettings", event.Stacktrace)
	}
	if event.Type != "*fmt.wrapError" {
		t.Errorf("type = %q", event.Type)
	}

	chain, _ := event.Extra["error_chain"].([]map[string]string)
	if len(chain) != 1 || chain[0]["type"] != "*errors.errorString" {
		t.Errorf("chain = %v, expected the recorder to be left out", chain)
	}
}

func TestErrorfNamesTheWrappedTypeAndKeepsTheDeepestStack(t *testing.T) {
	inner := loadSettings()
	outer := WithStack(Errorf("handler: %w", inner))

	if errorType(outer) != "*fmt.wrapError" {
		t.Errorf("type = %q, expected the wrapped error's type", errorType(outer))
	}
	frames := errorFrames(outer)
	if len(frames) == 0 || !strings.HasSuffix(frames[0].Function, ".loadSettings") {
		t.Errorf("frames start at %+v, expected the deepest recorded stack", frames)
	}
	if !errors.Is(outer, inner) {
		t.Error("the recorder must keep the chain intact for errors.Is")
	}
	if WithStack(nil) != nil {
		t.Error("WithStack(nil) must stay nil")
	}
}

func TestRepeatedErrorIsHeldBackAndCounted(t *testing.T) {
	client, transport := newTestClient(t, func(options *Options) {
		options.DedupeWindow = time.Hour
	})

	for range 5 {
		client.CaptureException(errors.New("database is down"))
	}
	client.CaptureException(errors.New("a different error"))

	transport.mu.Lock()
	sent := len(transport.events)
	transport.mu.Unlock()
	if sent != 2 {
		t.Fatalf("sent %d events, expected the first copy and the other error", sent)
	}
	if got := client.Stats().Duplicates; got != 4 {
		t.Errorf("duplicates = %d, expected 4", got)
	}

	// Once the window is over, the next copy reports what was held back.
	client.dedupe.window = time.Nanosecond
	time.Sleep(time.Millisecond)
	client.CaptureException(errors.New("database is down"))
	if repeats := transport.last().Extra["repeats_dropped"]; repeats != 4 {
		t.Errorf("repeats_dropped = %v, expected 4", repeats)
	}
}

func TestNegativeDedupeWindowSendsEveryCopy(t *testing.T) {
	client, transport := newTestClient(t, func(options *Options) {
		options.DedupeWindow = -1
	})
	for range 3 {
		client.CaptureException(errors.New("same"))
	}
	transport.mu.Lock()
	defer transport.mu.Unlock()
	if len(transport.events) != 3 {
		t.Errorf("sent %d events, expected 3", len(transport.events))
	}
}

func TestSampledOutEventsAreCountedAndNotBuilt(t *testing.T) {
	client, transport := newTestClient(t, func(options *Options) {
		options.SampleRate = 0.000001
	})
	for range 50 {
		client.CaptureMessage(LevelInfo, "rare")
	}
	if transport.last() != nil {
		t.Skip("the one-in-a-million sample was kept")
	}
	if got := client.Stats().Sampled; got != 50 {
		t.Errorf("sampled = %d, expected 50", got)
	}
}

func TestMemoryStatsReadRuntimeMetrics(t *testing.T) {
	stats := memoryStats()
	if goroutines, _ := stats["goroutines"].(uint64); goroutines == 0 {
		t.Errorf("stats = %v, expected a goroutine count", stats)
	}
}
