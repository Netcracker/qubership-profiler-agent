package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// vmClient queries the VictoriaMetrics HTTP API (Prometheus-compatible
// /api/v1/query and /api/v1/query_range).
type vmClient struct {
	base string
	http *http.Client
}

func newVMClient(base string) *vmClient {
	return &vmClient{base: base, http: &http.Client{Timeout: 30 * time.Second}}
}

type promResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Metric map[string]string `json:"metric"`
			Value  []any             `json:"value"`  // instant: [ts, "v"]
			Values [][]any           `json:"values"` // range: [[ts, "v"], ...]
		} `json:"result"`
	} `json:"data"`
	Error string `json:"error"`
}

func (c *vmClient) get(ctx context.Context, path string, params url.Values) (*promResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+path+"?"+params.Encode(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: HTTP %d: %s", path, resp.StatusCode, truncate(body, 200))
	}
	var pr promResponse
	if err := json.Unmarshal(body, &pr); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if pr.Status != "success" {
		return nil, fmt.Errorf("%s: %s", path, pr.Error)
	}
	return &pr, nil
}

// Instant runs an instant query and reduces the result to one number (the
// spec queries aggregate already; extra series are summed as a guard against
// under-aggregated queries). ok=false when the result is empty — absent
// series must not read as zero.
func (c *vmClient) Instant(ctx context.Context, query string) (float64, bool, error) {
	pr, err := c.get(ctx, "/api/v1/query", url.Values{"query": {query}})
	if err != nil {
		return 0, false, err
	}
	sum, seen := 0.0, false
	for _, r := range pr.Data.Result {
		if len(r.Value) != 2 {
			continue
		}
		v, err := toFloat(r.Value[1])
		if err != nil {
			return 0, false, err
		}
		sum += v
		seen = true
	}
	return sum, seen, nil
}

// vecSample is one series of an instant-query result, labels included.
type vecSample struct {
	Metric map[string]string
	Value  float64
}

// InstantVector runs an instant query and returns every series with its
// labels — for results whose labels carry the payload (the workload
// fingerprint), where Instant's sum-and-collapse would destroy them.
func (c *vmClient) InstantVector(ctx context.Context, query string) ([]vecSample, error) {
	pr, err := c.get(ctx, "/api/v1/query", url.Values{"query": {query}})
	if err != nil {
		return nil, err
	}
	out := make([]vecSample, 0, len(pr.Data.Result))
	for _, r := range pr.Data.Result {
		if len(r.Value) != 2 {
			continue
		}
		v, err := toFloat(r.Value[1])
		if err != nil {
			return nil, err
		}
		out = append(out, vecSample{Metric: r.Metric, Value: v})
	}
	return out, nil
}

// Range exports a query over [from, to] for the series/ artifacts; the raw
// JSON body is stored, not interpreted.
func (c *vmClient) Range(ctx context.Context, query string, from, to time.Time, step time.Duration) (json.RawMessage, error) {
	pr, err := c.get(ctx, "/api/v1/query_range", url.Values{
		"query": {query},
		"start": {strconv.FormatInt(from.Unix(), 10)},
		"end":   {strconv.FormatInt(to.Unix(), 10)},
		"step":  {strconv.Itoa(int(step.Seconds()))},
	})
	if err != nil {
		return nil, err
	}
	return json.Marshal(pr)
}

func toFloat(v any) (float64, error) {
	s, ok := v.(string)
	if !ok {
		return 0, fmt.Errorf("unexpected sample value %v", v)
	}
	return strconv.ParseFloat(s, 64)
}

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "..."
}
