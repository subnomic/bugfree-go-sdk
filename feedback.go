package bugfree

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// Feedback is what a user wrote about a problem, forwarded from a form of the
// application's own.
type Feedback struct {
	// EventID ties the feedback to the event the user saw: the id a capture call
	// returned, shown to the user as a reference.
	EventID string `json:"event_id,omitempty"`
	Name    string `json:"name,omitempty"`
	Email   string `json:"email,omitempty"`
	Message string `json:"message"`
	// URL is the page or screen the user wrote from.
	URL         string `json:"url,omitempty"`
	Environment string `json:"environment,omitempty"`
	Release     string `json:"release,omitempty"`
}

// CaptureFeedback sends feedback and reports whether the server stored it. It
// waits for the answer, at most five seconds.
func (c *Client) CaptureFeedback(feedback Feedback) error {
	if !c.Enabled() {
		return nil
	}
	if feedback.Message == "" {
		return fmt.Errorf("bugfree: the feedback message is empty")
	}
	if feedback.Environment == "" {
		feedback.Environment = c.options.Environment
	}
	if feedback.Release == "" {
		feedback.Release = c.options.Release
	}

	body, err := json.Marshal(feedback)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), checkInTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.dsn.base+"/feedback", bytes.NewReader(body))
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
	if response.StatusCode >= 300 {
		answer, _ := io.ReadAll(io.LimitReader(response.Body, 8*1024))
		return fmt.Errorf("bugfree: feedback was refused (%d): %s", response.StatusCode, answer)
	}
	return nil
}

// CaptureFeedback sends feedback with the global client.
func CaptureFeedback(feedback Feedback) error {
	return Current().CaptureFeedback(feedback)
}
