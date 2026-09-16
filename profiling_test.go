package bugfree

import (
	"encoding/base64"
	"encoding/json"
	"sync"
	"testing"
	"time"
)

type profileTransport struct {
	recordingTransport
	mu       sync.Mutex
	profiles []map[string]any
}

func (t *profileTransport) SendProfile(body []byte) {
	var profile map[string]any
	_ = json.Unmarshal(body, &profile)
	t.mu.Lock()
	defer t.mu.Unlock()
	t.profiles = append(t.profiles, profile)
}

func (t *profileTransport) kinds() map[string]int {
	t.mu.Lock()
	defer t.mu.Unlock()
	kinds := map[string]int{}
	for _, profile := range t.profiles {
		kinds[profile["kind"].(string)]++
	}
	return kinds
}

func TestProfilerSendsCPUAndHeapProfiles(t *testing.T) {
	transport := &profileTransport{}
	client, err := NewClient(Options{
		DSN: "http://key@localhost/ingest", Release: "api@3", Transport: transport,
		ProfilingInterval: 50 * time.Millisecond, ProfileDuration: 30 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for transport.kinds()["heap"] == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	client.Close()

	kinds := transport.kinds()
	if kinds["cpu"] == 0 || kinds["heap"] == 0 {
		t.Fatalf("kinds = %v", kinds)
	}
	transport.mu.Lock()
	defer transport.mu.Unlock()
	for _, profile := range transport.profiles {
		data, err := base64.StdEncoding.DecodeString(profile["data"].(string))
		if err != nil || len(data) == 0 || profile["format"] != "pprof" || profile["release"] != "api@3" {
			t.Errorf("profile = %v (%d bytes, %v)", profile["kind"], len(data), err)
		}
	}
}

func TestProfilingIsOffByDefault(t *testing.T) {
	client, _ := newTestClient(t, nil)
	if client.profiler != nil {
		t.Error("profiling has to be asked for")
	}
}
