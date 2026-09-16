package bugfree

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"time"
)

// CheckInStatus is where a monitored job's run stands.
type CheckInStatus string

// The statuses a job reports.
const (
	CheckInInProgress CheckInStatus = "in_progress"
	CheckInOK         CheckInStatus = "ok"
	CheckInError      CheckInStatus = "error"
)

// MonitorConfig creates the monitor on its first check-in, or updates it, so the
// schedule lives next to the job's code. Set Schedule (a crontab expression) or
// IntervalValue and IntervalUnit ("minute", "hour", "day", "week").
type MonitorConfig struct {
	Name          string
	Schedule      string
	IntervalValue int
	IntervalUnit  string
	// Timezone is the IANA name the crontab expression is read in; UTC when empty.
	Timezone string
	// CheckInMargin is how late a check-in may be before it counts as missed.
	CheckInMargin time.Duration
	// MaxRuntime is how long a run may stay in progress before it times out.
	MaxRuntime time.Duration
}

// MarshalJSON writes the configuration in the form the server reads.
func (m MonitorConfig) MarshalJSON() ([]byte, error) {
	scheduleType := "crontab"
	if m.Schedule == "" && m.IntervalValue > 0 {
		scheduleType = "interval"
	}
	return json.Marshal(struct {
		Name                 string `json:"name,omitempty"`
		ScheduleType         string `json:"schedule_type"`
		Schedule             string `json:"schedule,omitempty"`
		IntervalValue        int    `json:"interval_value,omitempty"`
		IntervalUnit         string `json:"interval_unit,omitempty"`
		Timezone             string `json:"timezone,omitempty"`
		CheckInMarginMinutes int    `json:"checkin_margin_minutes,omitempty"`
		MaxRuntimeMinutes    int    `json:"max_runtime_minutes,omitempty"`
	}{
		Name:                 m.Name,
		ScheduleType:         scheduleType,
		Schedule:             m.Schedule,
		IntervalValue:        m.IntervalValue,
		IntervalUnit:         m.IntervalUnit,
		Timezone:             m.Timezone,
		CheckInMarginMinutes: wholeMinutes(m.CheckInMargin),
		MaxRuntimeMinutes:    wholeMinutes(m.MaxRuntime),
	})
}

// wholeMinutes rounds a duration up to minutes; the server counts in minutes.
func wholeMinutes(duration time.Duration) int {
	if duration <= 0 {
		return 0
	}
	return int((duration + time.Minute - 1) / time.Minute)
}

// CheckIn is one report of a monitored job.
type CheckIn struct {
	// ID pairs the end of a run with its start. CaptureCheckIn fills it in when it
	// is empty; pass the id it returned with the end of the run.
	ID string `json:"check_in_id"`
	// MonitorSlug names the monitor: lowercase letters, digits, - and _.
	MonitorSlug string        `json:"-"`
	Status      CheckInStatus `json:"status"`
	// Duration is how long the run took; measured by the server from the start
	// check-in when zero.
	Duration      time.Duration  `json:"-"`
	DurationMS    *int64         `json:"duration_ms,omitempty"`
	Environment   string         `json:"environment,omitempty"`
	Release       string         `json:"release,omitempty"`
	MonitorConfig *MonitorConfig `json:"monitor_config,omitempty"`
}

// CheckInSender is a Transport that also delivers check-ins, as the SDK's own
// test transports do. Without it the client sends check-ins over HTTP itself.
type CheckInSender interface {
	SendCheckIn(checkIn *CheckIn)
}

// checkInTimeout bounds one check-in request.
const checkInTimeout = 5 * time.Second

// CaptureCheckIn reports a monitored job's run and returns the check-in's id.
//
// It is sent at once and waits for the answer (at most five seconds): a job that
// checks in is often a short program that exits right after, and a queued
// check-in would die with it. The id is empty when the client is disabled.
func (c *Client) CaptureCheckIn(checkIn CheckIn) string {
	if !c.Enabled() || checkIn.MonitorSlug == "" {
		return ""
	}
	if checkIn.ID == "" {
		checkIn.ID = newEventID()
	}
	if checkIn.Environment == "" {
		checkIn.Environment = c.options.Environment
	}
	if checkIn.Release == "" {
		checkIn.Release = c.options.Release
	}
	if checkIn.Duration > 0 {
		milliseconds := checkIn.Duration.Milliseconds()
		checkIn.DurationMS = &milliseconds
	}

	if sender, ok := c.transport.(CheckInSender); ok {
		sender.SendCheckIn(&checkIn)
		return checkIn.ID
	}
	if transport, ok := c.transport.(*httpTransport); ok && transport.paused() {
		transport.droppedPaused.Add(1)
		return checkIn.ID
	}
	if err := c.postCheckIn(&checkIn); err != nil && c.options.Debug {
		log.Printf("bugfree: check-in of %s could not be sent: %v", checkIn.MonitorSlug, err)
	}
	return checkIn.ID
}

// postCheckIn delivers a check-in over HTTP.
func (c *Client) postCheckIn(checkIn *CheckIn) error {
	body, err := json.Marshal(checkIn)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), checkInTimeout)
	defer cancel()
	address := fmt.Sprintf("%s/monitors/%s/checkins", c.dsn.base, url.PathEscape(checkIn.MonitorSlug))
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, address, bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("User-Agent", "bugfree-go/"+Version)

	client := c.options.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: checkInTimeout}
	}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	answer, _ := io.ReadAll(io.LimitReader(response.Body, 8*1024))

	if response.StatusCode == http.StatusTooManyRequests {
		if transport, ok := c.transport.(*httpTransport); ok {
			transport.pause(parseRetryAfter(response.Header.Get("Retry-After"), time.Now()))
		}
	}
	if response.StatusCode >= 300 {
		return fmt.Errorf("status %d: %s", response.StatusCode, answer)
	}
	return nil
}

// WithMonitor runs job as one run of the monitor: it checks in when the job
// starts and when it ends, with ok or error by the job's result. A panic is
// reported as a failed run and raised again. config may be nil once the monitor
// exists.
//
//	err := bugfree.WithMonitor("nightly-backup", &bugfree.MonitorConfig{Schedule: "0 3 * * *"}, backup)
func (c *Client) WithMonitor(slug string, config *MonitorConfig, job func() error) error {
	started := time.Now()
	id := c.CaptureCheckIn(CheckIn{MonitorSlug: slug, Status: CheckInInProgress, MonitorConfig: config})

	finish := func(status CheckInStatus) {
		c.CaptureCheckIn(CheckIn{ID: id, MonitorSlug: slug, Status: status, Duration: time.Since(started)})
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			finish(CheckInError)
			panic(recovered)
		}
	}()

	err := job()
	if err != nil {
		finish(CheckInError)
	} else {
		finish(CheckInOK)
	}
	return err
}

// CaptureCheckIn reports a monitored job's run with the global client.
func CaptureCheckIn(checkIn CheckIn) string {
	return Current().CaptureCheckIn(checkIn)
}

// WithMonitor runs job as one run of the monitor, with the global client.
func WithMonitor(slug string, config *MonitorConfig, job func() error) error {
	return Current().WithMonitor(slug, config, job)
}
