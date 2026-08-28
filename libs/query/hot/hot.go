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
	"github.com/Netcracker/qubership-profiler-backend/libs/query/budget"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	"github.com/pkg/errors"
)

// Body caps and the chunk step of the budgeted body reader (02 §7.5). The
// caps are a hard backstop, not a tuning knob: a replica that streams more
// than this into one response is broken, and an unbounded io.ReadAll here
// was itself an OOM vector. Implementation-local values.
const (
	// maxHotBlobBytes caps one /internal trace blob response.
	maxHotBlobBytes = 256 << 20
	// maxHotResponseBytes caps one /internal/v1/calls page response; a page
	// is at most MaxLimit rows, so this is generous.
	maxHotResponseBytes = 64 << 20
	// bodyChunkBytes is the reserve-before-read step for a response without
	// Content-Length: fixed-size steps, no geometric growth, each reserved
	// against the budget BEFORE the read that fills it.
	bodyChunkBytes = 1 << 20
	// hotDecodeReserveFactor gates the /calls decode peak. json.Unmarshal
	// builds body.Calls and the converted rows while the raw JSON buffer is
	// still alive, so up to ~3x the body coexists at the peak. The lease
	// already holds the raw body (1x); reserving this many more up front makes
	// admission — not the allocator — shed the peak, and the reconcile trues
	// the charge back down to the surviving rows (02 §7.5).
	hotDecodeReserveFactor = 2
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
	// Response bodies of the two unbounded endpoints (calls pages, trace
	// blobs) are read through the §7.5 read budget; a nil budget disables the
	// charging but keeps the size caps.
	Client struct {
		http   *http.Client
		budget *budget.Budget
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
			// EndMs is the pause end; the pause spans [EndMs − DurationMs, EndMs]
			// (calltree treats SuspendInterval.TimeMs as the end) (№4).
			EndMs      int64 `json:"end_ms"`
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

// NewClient builds the fan-out HTTP client (PROFILER_FANOUT_TIMEOUT, §7.2)
// drawing large response bodies from b (02 §7.5; nil disables charging).
func NewClient(timeout time.Duration, b *budget.Budget) *Client {
	return &Client{http: &http.Client{Timeout: timeout}, budget: b}
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
//
// The response body is read through the budget and the returned lease owns
// the decoded rows' footprint (02 §7.5); the caller moves it into the
// request page lease and releases it. On any error the lease is already
// released.
func (c *Client) Calls(ctx context.Context, baseURL string, q model.CallsQuery, after *model.Position, limit int) ([]model.CallRow, *budget.Lease, error) {
	v := q.Values()
	if after != nil {
		v.Set("after_ts_ms", strconv.FormatInt(after.TsMs, 10))
		v.Set("after_pk", after.PK.PathString())
	}
	v.Set("limit", strconv.Itoa(limit))
	u := baseURL + "/internal/v1/calls?" + v.Encode()

	raw, lease, err := c.getBudgeted(ctx, u, maxHotResponseBytes)
	if err != nil {
		return nil, nil, err
	}
	// Gate the decode peak, not just the raw body: the raw buffer, the decoder's
	// intermediate body.Calls, and the converted rows all coexist until the
	// reconcile below, so charge a conservative multiple of the raw size BEFORE
	// decoding (02 §7.5). A denial here sheds the request with 503 like any
	// other, instead of letting the allocator run past the budget.
	if err := lease.Grow(ctx, int64(len(raw))*hotDecodeReserveFactor); err != nil {
		lease.Release()
		return nil, nil, err
	}
	var body callsBody
	if err := json.Unmarshal(raw, &body); err != nil {
		lease.Release()
		return nil, nil, errors.Wrapf(err, "decode %s", u)
	}
	rows := make([]model.CallRow, 0, len(body.Calls))
	for _, call := range body.Calls {
		rows = append(rows, call.Row(model.TierHot))
	}
	// Reconcile to the decoded rows: the raw buffer and the decode reservation
	// are dropped here, the rows live on under the lease.
	var footprint int64
	for i := range rows {
		footprint += model.RowFootprint(&rows[i])
	}
	if held := lease.Held(); footprint > held {
		if err := lease.Grow(ctx, footprint-held); err != nil {
			lease.Release()
			return nil, nil, err
		}
	} else {
		lease.Shrink(held - footprint)
	}
	return rows, lease, nil
}

// getBudgeted GETs one URL and reads its body against the §7.5 budget with
// the given hard cap: a declared Content-Length is charged up front (and
// rejected past the cap before any read); a chunked body is reserved in
// fixed steps BEFORE each read. The returned lease owns the body bytes.
func (c *Client) getBudgeted(ctx context.Context, u string, maxBytes int64) ([]byte, *budget.Lease, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, nil, errors.Wrap(err, "build replica request")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return nil, nil, fmt.Errorf("GET %s: %s: %s", u, resp.Status, snippet)
	}
	body, lease, err := c.readBudgetedBody(ctx, resp, maxBytes)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "read %s", u)
	}
	return body, lease, nil
}

func (c *Client) readBudgetedBody(ctx context.Context, resp *http.Response, maxBytes int64) ([]byte, *budget.Lease, error) {
	if cl := resp.ContentLength; cl >= 0 {
		if cl > maxBytes {
			return nil, nil, errors.Errorf("body of %d bytes exceeds the %d-byte cap", cl, maxBytes)
		}
		lease, err := c.budget.Acquire(ctx, cl)
		if err != nil {
			return nil, nil, err
		}
		buf := make([]byte, cl)
		if _, err := io.ReadFull(resp.Body, buf); err != nil {
			lease.Release()
			return nil, nil, err
		}
		return buf, lease, nil
	}

	// No declared length: fixed reserve-before-read steps, assembled at the
	// end under a second charge for the contiguous copy (peak 2n, accounted),
	// then shrunk back to the body size.
	lease := c.budget.NewLease()
	var chunks [][]byte
	var total int64
	for {
		// Bound each read by the remaining allowance so total can never exceed
		// maxBytes: a full bodyChunkBytes read near the cap would otherwise
		// overshoot it by almost one chunk before the next check caught it.
		step := maxBytes - total
		if step <= 0 {
			// Exactly at the cap. One probe read tells a body of exactly
			// maxBytes (EOF now) from one over it (a byte still waiting), so the
			// cap accepts maxBytes and rejects only what exceeds it — matching
			// the Content-Length branch's `cl > maxBytes`. The probe byte is
			// neither retained nor charged, so the memory bound still holds.
			var probe [1]byte
			pn, perr := io.ReadFull(resp.Body, probe[:])
			if pn > 0 {
				lease.Release()
				return nil, nil, errors.Errorf("body exceeds the %d-byte cap", maxBytes)
			}
			if perr != nil && !errors.Is(perr, io.EOF) {
				// Only a clean EOF confirms the body ended exactly at the cap. A
				// transport error at the boundary leaves it unconfirmed —
				// surface it rather than serve a possibly truncated body as
				// complete, matching how the main read loop treats errors.
				lease.Release()
				return nil, nil, errors.Wrapf(perr, "read body at the %d-byte cap", maxBytes)
			}
			break
		}
		if step > bodyChunkBytes {
			step = bodyChunkBytes
		}
		if err := lease.Grow(ctx, step); err != nil {
			lease.Release()
			return nil, nil, err
		}
		buf := make([]byte, step)
		n, err := io.ReadFull(resp.Body, buf)
		if n > 0 {
			chunks = append(chunks, buf[:n])
			total += int64(n)
		}
		lease.Shrink(step - int64(n))
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}
		if err != nil {
			lease.Release()
			return nil, nil, err
		}
	}
	if len(chunks) == 1 {
		return chunks[0], lease, nil
	}
	if err := lease.Grow(ctx, total); err != nil {
		lease.Release()
		return nil, nil, err
	}
	out := make([]byte, 0, total)
	for _, chunk := range chunks {
		out = append(out, chunk...)
	}
	chunks = nil
	lease.Shrink(total)
	return out, lease, nil
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
//
// The body is read through the budget under the maxHotBlobBytes cap; a
// Content-Length past the cap is rejected on the headers alone (02 §7.5).
// The returned lease owns the blob and must live as long as the blob does —
// the caller moves it into the request point lease.
func (c *Client) Trace(ctx context.Context, baseURL string, pk model.PK) (blob []byte, lease *budget.Lease, found bool, err error) {
	u := baseURL + "/internal/v1/calls/" + url.PathEscape(pk.PathString()) + "/trace"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, nil, false, errors.Wrap(err, "build replica request")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, nil, false, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusNotFound {
		return nil, nil, false, nil
	}
	if resp.StatusCode != http.StatusOK {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return nil, nil, false, fmt.Errorf("GET %s: %s: %s", u, resp.Status, snippet)
	}
	blob, lease, err = c.readBudgetedBody(ctx, resp, maxHotBlobBytes)
	if err != nil {
		return nil, nil, false, errors.Wrapf(err, "read %s", u)
	}
	return blob, lease, true, nil
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
		pauses = append(pauses, calltree.SuspendInterval{TimeMs: e.EndMs, DurationMs: e.DurationMs})
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
