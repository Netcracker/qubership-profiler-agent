package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// k6Client drives the k6 REST API (the externally-controlled executor).
type k6Client struct {
	base string
	http *http.Client
}

func newK6Client(base string) *k6Client {
	return &k6Client{base: base, http: &http.Client{Timeout: 15 * time.Second}}
}

// k6StatusBody carries only the attributes a PATCH should touch: k6 acts on
// every present field (a `paused: false` on an unpaused test is an error),
// so everything except the pointer that is set stays omitted.
type k6StatusBody struct {
	Data struct {
		Type       string `json:"type"`
		ID         string `json:"id"`
		Attributes struct {
			Paused  *bool `json:"paused,omitempty"`
			VUs     *int  `json:"vus,omitempty"`
			VUsMax  *int  `json:"vus-max,omitempty"`
			Running bool  `json:"running,omitempty"`
		} `json:"attributes"`
	} `json:"data"`
}

// ScaleVUs asks k6 for the target VU count. A nil error only means the
// request was accepted; the caller confirms the level through k6_vus.
func (c *k6Client) ScaleVUs(ctx context.Context, vus int) error {
	body := k6StatusBody{}
	body.Data.Type = "status"
	body.Data.ID = "default"
	body.Data.Attributes.VUs = &vus
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, c.base+"/v1/status", bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("k6 PATCH /v1/status: HTTP %d: %s", resp.StatusCode, truncate(msg, 200))
	}
	return nil
}

// Status reads the executor state (used for the startup sanity check).
func (c *k6Client) Status(ctx context.Context) (k6StatusBody, error) {
	var out k6StatusBody
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/v1/status", nil)
	if err != nil {
		return out, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(resp.Body)
		return out, fmt.Errorf("k6 GET /v1/status: HTTP %d: %s", resp.StatusCode, truncate(msg, 200))
	}
	return out, json.NewDecoder(resp.Body).Decode(&out)
}
