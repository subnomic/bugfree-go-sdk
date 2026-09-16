package bugfree

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"runtime/pprof"
	"sync"
	"time"
)

// defaultProfileDuration is how long a CPU profile runs unless told otherwise.
const defaultProfileDuration = 10 * time.Second

// ProfileSender is a Transport that also delivers profiles, as the SDK's own test
// transports do. body is the JSON the server reads.
type ProfileSender interface {
	SendProfile(body []byte)
}

// profiler takes a CPU profile and a heap profile every interval and sends them.
//
// A program can run one CPU profile at a time. When another one is running (the
// application's own pprof endpoint was asked for one), this round is skipped
// rather than taking it over.
type profiler struct {
	client   *Client
	interval time.Duration
	duration time.Duration

	stop chan struct{}
	done chan struct{}
	once sync.Once
}

func newProfiler(client *Client, interval, duration time.Duration) *profiler {
	if duration <= 0 {
		duration = defaultProfileDuration
	}
	// A profile longer than the pause between two would never pause.
	duration = min(duration, interval)
	p := &profiler{client: client, interval: interval, duration: duration, stop: make(chan struct{}), done: make(chan struct{})}
	go p.run()
	return p
}

func (p *profiler) run() {
	defer close(p.done)
	timer := time.NewTimer(p.interval)
	defer timer.Stop()
	for {
		select {
		case <-p.stop:
			return
		case <-timer.C:
		}
		p.profileCPU()
		p.profileHeap()
		timer.Reset(p.interval)
	}
}

func (p *profiler) close() {
	p.once.Do(func() { close(p.stop) })
	<-p.done
}

func (p *profiler) profileCPU() {
	var buffer bytes.Buffer
	started := time.Now()
	if err := pprof.StartCPUProfile(&buffer); err != nil {
		p.client.debugf("bugfree: CPU profile skipped: %v", err)
		return
	}
	select {
	case <-time.After(p.duration):
	case <-p.stop:
	}
	pprof.StopCPUProfile()
	p.send("cpu", buffer.Bytes(), started, time.Since(started))
}

func (p *profiler) profileHeap() {
	var buffer bytes.Buffer
	if err := pprof.Lookup("heap").WriteTo(&buffer, 0); err != nil {
		return
	}
	p.send("heap", buffer.Bytes(), time.Now(), 0)
}

func (p *profiler) send(kind string, data []byte, started time.Time, duration time.Duration) {
	body, err := json.Marshal(map[string]any{
		"kind":        kind,
		"format":      "pprof",
		"data":        base64.StdEncoding.EncodeToString(data),
		"started_at":  timestamp(started),
		"duration_ms": duration.Milliseconds(),
		"environment": p.client.options.Environment,
		"release":     p.client.options.Release,
		"server_name": p.client.options.ServerName,
		"platform":    "go",
	})
	if err != nil {
		return
	}
	if sender, ok := p.client.transport.(ProfileSender); ok {
		sender.SendProfile(body)
		return
	}
	if transport, ok := p.client.transport.(*httpTransport); ok && transport.paused() {
		return
	}
	if err := p.client.postProfile(body); err != nil {
		p.client.debugf("bugfree: a profile could not be sent: %v", err)
	}
}

func (c *Client) postProfile(body []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.dsn.base+"/profiles", bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("User-Agent", "bugfree-go/"+Version)

	client := c.options.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		return fmt.Errorf("status %d", response.StatusCode)
	}
	return nil
}

// debugf logs only when Debug is on.
func (c *Client) debugf(format string, args ...any) {
	if c.options.Debug {
		log.Printf(format, args...)
	}
}
