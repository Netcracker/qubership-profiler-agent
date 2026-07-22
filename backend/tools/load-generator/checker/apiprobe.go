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

	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
)

// apiProbeConfig is the §8.7 contract (doc/checker.md): freshness budget,
// marker sampling, per-class TTLs from the same PROFILER_RETENTION_* env the
// stand sets.
type apiProbeConfig struct {
	baseURL         string
	freshnessBudget time.Duration
	markerCount     int
	ttlMargin       time.Duration
	// ttlSettle is the post-TTL grace before -expect-ttl-deletion demands a
	// 404 (defaults to the §8.5 settle chain).
	ttlSettle         time.Duration
	expectTTLDeletion bool
	classTTL          map[string]time.Duration
}

// marker is one pre-soak call the probe re-fetches every tick until its TTL.
type marker struct {
	pk    model.PK
	tsMs  int64
	class string
}

// probeFinding pairs a §8.7 finding with the allowance signal that may make
// it expected; the marking happens at evaluation time, against the freshly
// reloaded fault log (doc/checker.md).
type probeFinding struct {
	f      finding
	signal string // freshness | markers
}

// apiState runs the §8.7 probes and keeps their outcome for the invariant.
type apiState struct {
	cfg     apiProbeConfig
	client  *http.Client
	started time.Time

	markers        []marker
	markersSampled bool
	// current holds the findings of the last poll; the invariant reads them.
	current []probeFinding
}

func newAPIState(cfg apiProbeConfig) *apiState {
	return &apiState{cfg: cfg, client: &http.Client{Timeout: 30 * time.Second}, started: time.Now()}
}

// findings marks each probe finding against its allowance signal by the
// probe's own observation time.
func (a *apiState) findings(faults *faultState) []finding {
	out := make([]finding, 0, len(a.current))
	for _, pf := range a.current {
		f := pf.f
		f.expected = faults != nil && faults.expected(pf.signal, f.observedAt)
		out = append(out, f)
	}
	return out
}

// poll runs one §8.7 tick. The error return is transport-level (feeds the
// scrape-gap tracker); invariant findings land in a.current, stamped with
// the probe time.
func (a *apiState) poll(ctx context.Context, now time.Time, pastWarmup bool) error {
	var out []probeFinding

	fresh, err := a.checkFreshness(ctx, now)
	if err != nil {
		return err
	}
	for _, f := range fresh {
		f.observedAt = now
		out = append(out, probeFinding{f: f, signal: "freshness"})
	}

	if pastWarmup && !a.markersSampled {
		if err := a.sampleMarkers(ctx, now); err != nil {
			return err
		}
	}
	markerFindings, err := a.pollMarkers(ctx, now)
	if err != nil {
		return err
	}
	for _, f := range markerFindings {
		f.observedAt = now
		out = append(out, probeFinding{f: f, signal: "markers"})
	}

	a.current = out
	return nil
}

// checkFreshness asserts the hot window keeps serving fresh rows: the newest
// ts_ms of the trailing freshness-budget window must be younger than the
// budget. Windows are integer Unix milliseconds — /api/v1 accepts nothing
// else. A guard rejection (400) of this small window is a §8.7 violation in
// its own right — the UI cannot list recent calls under the stand's own
// guards — not a transport gap.
func (a *apiState) checkFreshness(ctx context.Context, now time.Time) ([]finding, error) {
	from := now.Add(-a.cfg.freshnessBudget)
	calls, status, err := a.fetchCalls(ctx, from, now, 1)
	if err != nil {
		return nil, err
	}
	if status == http.StatusBadRequest {
		return []finding{{subject: a.cfg.baseURL,
			msg: fmt.Sprintf("guard rejected the %s freshness window (HTTP 400): recent calls are not listable under the stand's guards", a.cfg.freshnessBudget)}}, nil
	}
	if len(calls) == 0 {
		return []finding{{subject: a.cfg.baseURL,
			msg: fmt.Sprintf("no calls in the last %s while the generator is feeding", a.cfg.freshnessBudget)}}, nil
	}
	age := now.Sub(time.UnixMilli(calls[0].TsMs))
	if age > a.cfg.freshnessBudget {
		return []finding{{subject: a.cfg.baseURL,
			msg: fmt.Sprintf("newest call is %s old (freshness budget %s)", age.Round(time.Second), a.cfg.freshnessBudget)}}, nil
	}
	return nil, nil
}

// sampleMarkers records the pre-soak rows §8.7 keeps re-fetching: rows
// written since the probe started, i.e. during warm-up. The corrupted class
// is excluded: it is reserved and writer-less today
// (libs/query/model/tiers.go). An empty window retries next tick.
func (a *apiState) sampleMarkers(ctx context.Context, now time.Time) error {
	calls, _, err := a.fetchCalls(ctx, a.started, now, a.cfg.markerCount)
	if err != nil {
		return err
	}
	for _, c := range calls {
		if c.RetentionClass == model.RetentionCorrupted {
			continue
		}
		a.markers = append(a.markers, marker{pk: c.PK, tsMs: c.TsMs, class: c.RetentionClass})
	}
	if len(a.markers) > 0 {
		a.markersSampled = true
		fmt.Printf("%s checker: sampled %d markers for §8.7\n", now.Format(time.RFC3339), len(a.markers))
	}
	return nil
}

// pollMarkers fetches every live marker's trace. Old data must stay
// retrievable from cold until its TTL; past the TTL the marker leaves the
// set (and with -expect-ttl-deletion must first turn 404).
func (a *apiState) pollMarkers(ctx context.Context, now time.Time) ([]finding, error) {
	var out []finding
	kept := a.markers[:0]
	for _, m := range a.markers {
		ttl, ok := a.cfg.classTTL[m.class]
		if !ok {
			continue // unknown class: nothing to assert
		}
		age := now.Sub(time.UnixMilli(m.tsMs))
		status, err := a.fetchTraceStatus(ctx, m)
		if err != nil {
			return nil, err
		}
		subject := m.pk.PathString()
		switch {
		case age < ttl-a.cfg.ttlMargin:
			if status != http.StatusOK {
				out = append(out, finding{subject: subject,
					msg: fmt.Sprintf("trace answered %d for a %s-old %s marker (TTL %s)", status, age.Round(time.Second), m.class, ttl)})
			}
			kept = append(kept, m)
		case age < ttl:
			// Indeterminate: TTL deletion is already legal, absence is not
			// yet a violation.
			kept = append(kept, m)
		default: // past TTL
			if a.cfg.expectTTLDeletion && age > ttl+a.cfg.ttlSettle && status == http.StatusOK {
				out = append(out, finding{subject: subject,
					msg: fmt.Sprintf("%s marker still retrievable %s past its %s TTL", m.class, (age - ttl).Round(time.Second), ttl)})
				kept = append(kept, m) // keep watching until it disappears
				continue
			}
			// Leaves the set: expired and (if demanded) confirmed deleted.
		}
	}
	a.markers = kept
	return out, nil
}

// fetchCalls returns the rows and the HTTP status; 200 and 400 (a guard
// rejection — a probe result, not a transport failure) come back without an
// error.
func (a *apiState) fetchCalls(ctx context.Context, from, to time.Time, limit int) ([]model.CallJSON, int, error) {
	q := url.Values{}
	q.Set("from", strconv.FormatInt(from.UnixMilli(), 10))
	q.Set("to", strconv.FormatInt(to.UnixMilli(), 10))
	q.Set("limit", strconv.Itoa(limit))
	var resp struct {
		Calls []model.CallJSON `json:"calls"`
	}
	status, err := a.getJSON(ctx, "/api/v1/calls?"+q.Encode(), &resp)
	if err != nil {
		return nil, status, err
	}
	return resp.Calls, status, nil
}

func (a *apiState) fetchTraceStatus(ctx context.Context, m marker) (int, error) {
	q := url.Values{}
	q.Set("ts_ms", strconv.FormatInt(m.tsMs, 10))
	q.Set("retention_class", m.class)
	// GET, not HEAD: the route is registered for GET only (HEAD answers 405).
	path := "/api/v1/calls/" + url.PathEscape(m.pk.PathString()) + "/trace?" + q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.cfg.baseURL+path, nil)
	if err != nil {
		return 0, err
	}
	resp, err := a.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, nil
}

func (a *apiState) getJSON(ctx context.Context, path string, into any) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.cfg.baseURL+path, nil)
	if err != nil {
		return 0, err
	}
	resp, err := a.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	switch resp.StatusCode {
	case http.StatusOK:
		return resp.StatusCode, json.NewDecoder(resp.Body).Decode(into)
	case http.StatusBadRequest:
		// Guard rejections are probe results the caller judges.
		_, _ = io.Copy(io.Discard, resp.Body)
		return resp.StatusCode, nil
	default:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return resp.StatusCode, fmt.Errorf("%s: status %s: %s", path, resp.Status, body)
	}
}
