package bugfree

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// checkInServer records the check-ins it receives.
type checkInServer struct {
	mu       sync.Mutex
	paths    []string
	payloads []map[string]any
}

func newCheckInClient(t *testing.T) (*Client, *checkInServer) {
	t.Helper()
	received := &checkInServer{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		_ = json.NewDecoder(r.Body).Decode(&payload)
		received.mu.Lock()
		received.paths = append(received.paths, r.URL.Path)
		received.payloads = append(received.payloads, payload)
		received.mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(server.Close)

	client, err := NewClient(Options{
		DSN:         strings.Replace(server.URL, "http://", "http://key123@", 1) + "/ingest",
		Environment: "production",
		Release:     "jobs@2.0.0",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return client, received
}

func TestCaptureCheckInSendsToTheMonitorsEndpoint(t *testing.T) {
	client, received := newCheckInClient(t)

	id := client.CaptureCheckIn(CheckIn{
		MonitorSlug: "nightly-backup",
		Status:      CheckInInProgress,
		MonitorConfig: &MonitorConfig{
			Schedule:      "0 3 * * *",
			Timezone:      "Europe/Istanbul",
			CheckInMargin: 90 * time.Second,
			MaxRuntime:    time.Hour,
		},
	})
	if !uuidV4.MatchString(id) {
		t.Fatalf("id = %q", id)
	}

	if len(received.paths) != 1 || received.paths[0] != "/ingest/v1/key123/monitors/nightly-backup/checkins" {
		t.Fatalf("paths = %v", received.paths)
	}
	payload := received.payloads[0]
	config, _ := payload["monitor_config"].(map[string]any)
	if payload["check_in_id"] != id || payload["status"] != "in_progress" || payload["environment"] != "production" || payload["release"] != "jobs@2.0.0" {
		t.Errorf("payload = %v", payload)
	}
	if config["schedule_type"] != "crontab" || config["checkin_margin_minutes"] != float64(2) || config["max_runtime_minutes"] != float64(60) {
		t.Errorf("config = %v", config)
	}
}

func TestIntervalConfigIsSentAsAnInterval(t *testing.T) {
	body, err := json.Marshal(MonitorConfig{IntervalValue: 10, IntervalUnit: "minute"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), `"schedule_type":"interval"`) || strings.Contains(string(body), `"schedule":`) {
		t.Errorf("body = %s", body)
	}
}

func TestWithMonitorReportsStartAndResult(t *testing.T) {
	client, received := newCheckInClient(t)

	if err := client.WithMonitor("sync", nil, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("upstream refused")
	if err := client.WithMonitor("sync", nil, func() error { return failure }); !errors.Is(err, failure) {
		t.Fatalf("err = %v, the job's error has to come back", err)
	}
	func() {
		defer func() { _ = recover() }()
		_ = client.WithMonitor("sync", nil, func() error { panic("disk full") })
	}()

	var statuses []string
	for _, payload := range received.payloads {
		statuses = append(statuses, payload["status"].(string))
	}
	want := "in_progress ok in_progress error in_progress error"
	if got := strings.Join(statuses, " "); got != want {
		t.Fatalf("statuses = %q, expected %q", got, want)
	}
	// The end of a run carries the start's id and a duration.
	if received.payloads[1]["check_in_id"] != received.payloads[0]["check_in_id"] {
		t.Error("the end of the run does not pair with its start")
	}
	if _, ok := received.payloads[1]["duration_ms"]; !ok {
		t.Error("the end of the run carries no duration")
	}
}

func TestWithMonitorRunsTheJobWhenDisabled(t *testing.T) {
	disabled, _ := NewClient(Options{})
	ran := false
	if err := disabled.WithMonitor("sync", nil, func() error { ran = true; return nil }); err != nil || !ran {
		t.Errorf("ran = %v, err = %v", ran, err)
	}
	var none *Client
	if err := none.WithMonitor("sync", nil, func() error { return nil }); err != nil {
		t.Errorf("a nil client must still run the job: %v", err)
	}
}

func TestCaptureFeedbackPostsToTheFeedbackEndpoint(t *testing.T) {
	client, received := newCheckInClient(t)

	err := client.CaptureFeedback(Feedback{EventID: "0b4c6d1e-8f2a-4b3c-9d5e-6f7a8b9c0d1e", Email: "jane@example.com", Message: "The export never finished"})
	if err != nil {
		t.Fatal(err)
	}
	if len(received.paths) != 1 || received.paths[0] != "/ingest/v1/key123/feedback" {
		t.Fatalf("paths = %v", received.paths)
	}
	if payload := received.payloads[0]; payload["message"] != "The export never finished" || payload["release"] != "jobs@2.0.0" {
		t.Errorf("payload = %v", payload)
	}
	if err := client.CaptureFeedback(Feedback{}); err == nil {
		t.Error("an empty message was sent")
	}
}
