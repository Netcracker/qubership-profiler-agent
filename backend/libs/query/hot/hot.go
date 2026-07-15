// Package hot is the query service's client side of the collector fan-out
// (02-read-contract.md §3, §7): replica discovery through the headless
// Service and the per-replica /internal/v1 reads whose rows feed the tier
// merge. It holds no per-replica state — the fan-out is re-issued whole on
// every page (§2.3.1).
package hot

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/calltree"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	"github.com/pkg/errors"
)

type (
	// Discovery lists the base URLs of the Ready collector replicas. The DNS
	// implementation re-resolves on every request (02 §7.1); tests supply a
	// static or scripted list.
	Discovery interface {
		Replicas(ctx context.Context) ([]string, error)
	}

	// DNSDiscovery resolves the headless Service to one A record per Ready
	// replica (COLLECTOR_HEADLESS_SVC, 02 §7.1; only Ready pods are published,
	// 04-storage-layout.md §3.3).
	DNSDiscovery struct {
		Service string
		Port    int
	}

	// Window is one replica's /internal/v1/health/hot-window report (02 §3),
	// the input to the dynamic cold cutoff (§4.3).
	Window struct {
		OldestMs int64 `json:"hot_window_oldest_ms"`
		NowMs    int64 `json:"hot_window_now_ms"`
	}

	// Client issues the per-replica reads with the §7.2 per-request timeout.
	Client struct {
		http *http.Client
	}

	callsBody struct {
		Calls []model.CallJSON `json:"calls"`
	}

	podsBody struct {
		Pods []model.PodEntry `json:"pods"`
	}

	dictionaryBody struct {
		Version int      `json:"version"`
		Methods []string `json:"methods"`
	}

	valuesBody struct {
		Values map[string]string `json:"values"`
	}

	suspendBody struct {
		Events []struct {
			StartMs    int64 `json:"start_ms"`
			DurationMs int64 `json:"duration_ms"`
		} `json:"events"`
	}

	// Dictionary is one replica's §2.6 snapshot plus the ETag the caller's
	// per-pod-restart cache revalidates with (If-None-Match → 304).
	Dictionary struct {
		Words []string
		ETag  string
	}
)

// Replicas resolves one base URL per Ready collector pod, in a stable order.
func (d DNSDiscovery) Replicas(ctx context.Context) ([]string, error) {
	addrs, err := net.DefaultResolver.LookupHost(ctx, d.Service)
	if err != nil {
		return nil, errors.Wrapf(err, "resolve %s", d.Service)
	}
	sort.Strings(addrs)
	urls := make([]string, 0, len(addrs))
	for _, addr := range addrs {
		urls = append(urls, "http://"+net.JoinHostPort(addr, strconv.Itoa(d.Port)))
	}
	return urls, nil
}

// NewClient builds the fan-out HTTP client (PROFILER_FANOUT_TIMEOUT, §7.2).
func NewClient(timeout time.Duration) *Client {
	return &Client{http: &http.Client{Timeout: timeout}}
}

// HotWindow fetches one replica's hot-window report.
func (c *Client) HotWindow(ctx context.Context, baseURL string) (Window, error) {
	var w Window
	err := c.getJSON(ctx, baseURL+"/internal/v1/health/hot-window", &w)
	return w, err
}

// Calls fetches one replica's rows past the cursor position: the same
// parameters as /api/v1/calls plus the keyset (02 §3, §2.3.1). The replica
// returns them already in the shared (ts_ms DESC, pk ASC) order, so the
// result plugs into the k-way merge as one run.
func (c *Client) Calls(ctx context.Context, baseURL string, q model.CallsQuery, after *model.Position, limit int) ([]model.CallRow, error) {
	v := q.Values()
	if after != nil {
		v.Set("after_ts_ms", strconv.FormatInt(after.TsMs, 10))
		v.Set("after_pk", after.PK.PathString())
	}
	v.Set("limit", strconv.Itoa(limit))
	var body callsBody
	if err := c.getJSON(ctx, baseURL+"/internal/v1/calls?"+v.Encode(), &body); err != nil {
		return nil, err
	}
	rows := make([]model.CallRow, 0, len(body.Calls))
	for _, call := range body.Calls {
		rows = append(rows, call.Row(model.TierHot))
	}
	return rows, nil
}

// Pods fetches the pod-restarts one replica holds data for in [fromMs, toMs)
// — the hot half of the §2.7 union.
func (c *Client) Pods(ctx context.Context, baseURL string, fromMs, toMs int64) ([]model.PodEntry, error) {
	v := url.Values{}
	v.Set("from", strconv.FormatInt(fromMs, 10))
	v.Set("to", strconv.FormatInt(toMs, 10))
	var body podsBody
	if err := c.getJSON(ctx, baseURL+"/internal/v1/pods?"+v.Encode(), &body); err != nil {
		return nil, err
	}
	return body.Pods, nil
}

// Trace fetches one call's raw blob from a replica. found is false on 404 —
// the §2.4 "absent" state, which for a fan-out probe just means the next
// source is asked.
func (c *Client) Trace(ctx context.Context, baseURL string, pk model.PK) (blob []byte, found bool, err error) {
	u := baseURL + "/internal/v1/calls/" + url.PathEscape(pk.PathString()) + "/trace"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, false, errors.Wrap(err, "build replica request")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusNotFound {
		return nil, false, nil
	}
	if resp.StatusCode != http.StatusOK {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return nil, false, fmt.Errorf("GET %s: %s: %s", u, resp.Status, snippet)
	}
	blob, err = io.ReadAll(resp.Body)
	return blob, err == nil, errors.Wrapf(err, "read %s", u)
}

// FetchDictionary fetches one live pod-restart's dictionary from the replica
// hosting it (02 §2.6, §3), revalidating a cached copy when etag is
// non-empty. notModified reports a 304; found is false when the replica does
// not host the pod-restart.
func (c *Client) FetchDictionary(ctx context.Context, baseURL string, tuple model.PodTuple, etag string) (dict Dictionary, notModified, found bool, err error) {
	u := baseURL + "/internal/v1/pods/" + url.PathEscape(podRestartPath(tuple)) + "/dictionary"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return dict, false, false, errors.Wrap(err, "build replica request")
	}
	if etag != "" {
		req.Header.Set("If-None-Match", etag)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return dict, false, false, err
	}
	defer func() { _ = resp.Body.Close() }()
	switch resp.StatusCode {
	case http.StatusNotModified:
		return dict, true, true, nil
	case http.StatusNotFound:
		return dict, false, false, nil
	case http.StatusOK:
		var body dictionaryBody
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			return dict, false, false, errors.Wrapf(err, "decode %s", u)
		}
		return Dictionary{Words: body.Methods, ETag: resp.Header.Get("ETag")}, false, true, nil
	default:
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return dict, false, false, fmt.Errorf("GET %s: %s: %s", u, resp.Status, snippet)
	}
}

// Suspend fetches the pod-restart's stop-the-world timeline from the replica
// hosting it (08-ui-backend-requirements.md R7). found is false on 404 — the
// pod-restart left this replica between the blob fetch and this call, and
// the caller degrades to zero suspension rather than failing the tree.
func (c *Client) Suspend(ctx context.Context, baseURL string, tuple model.PodTuple) (pauses []calltree.SuspendInterval, found bool, err error) {
	u := baseURL + "/internal/v1/pods/" + url.PathEscape(podRestartPath(tuple)) + "/suspend"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, false, errors.Wrap(err, "build replica request")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusNotFound {
		return nil, false, nil
	}
	if resp.StatusCode != http.StatusOK {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return nil, false, fmt.Errorf("GET %s: %s: %s", u, resp.Status, snippet)
	}
	var body suspendBody
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, false, errors.Wrapf(err, "decode %s", u)
	}
	pauses = make([]calltree.SuspendInterval, 0, len(body.Events))
	for _, e := range body.Events {
		pauses = append(pauses, calltree.SuspendInterval{TimeMs: e.StartMs, DurationMs: e.DurationMs})
	}
	return pauses, true, nil
}

// Values fetches big-parameter values from the replica's sql / xml value
// segments in one batch (01 §4.4). Unresolvable references are absent from
// the result, matching the endpoint's degrade-not-fail semantics.
func (c *Client) Values(ctx context.Context, baseURL string, tuple model.PodTuple, refs []string) (map[string]string, error) {
	v := url.Values{"ref": refs}
	u := baseURL + "/internal/v1/pods/" + url.PathEscape(podRestartPath(tuple)) + "/values?" + v.Encode()
	var body valuesBody
	if err := c.getJSON(ctx, u, &body); err != nil {
		return nil, err
	}
	return body.Values, nil
}

// podRestartPath renders the §2.6 path segment <ns>:<svc>:<pod>:<restartMs>.
func podRestartPath(t model.PodTuple) string {
	return t.Namespace + ":" + t.Service + ":" + t.Pod + ":" + strconv.FormatInt(t.RestartTimeMs, 10)
}

func (c *Client) getJSON(ctx context.Context, url string, into any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return errors.Wrap(err, "build replica request")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return fmt.Errorf("GET %s: %s: %s", url, resp.Status, snippet)
	}
	return errors.Wrapf(json.NewDecoder(resp.Body).Decode(into), "decode %s", url)
}
