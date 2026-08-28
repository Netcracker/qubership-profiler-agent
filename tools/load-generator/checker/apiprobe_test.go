package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
)

// fakeQuery is an httptest /api/v1 stub: a fixed calls list plus a
// per-marker trace status.
type fakeQuery struct {
	calls       []model.CallJSON
	traceStatus map[string]int // pk path -> status
	guardCalls  bool           // reject /calls with 400, like the cost guard
}

func (f *fakeQuery) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/calls", func(w http.ResponseWriter, r *http.Request) {
		if f.guardCalls {
			http.Error(w, `{"title":"scan budget exceeded"}`, http.StatusBadRequest)
			return
		}
		// The §8.7 contract sends integer Unix milliseconds; reject anything
		// else the way ParseWindow does.
		for _, key := range []string{"from", "to"} {
			if _, err := strconv.ParseInt(r.URL.Query().Get(key), 10, 64); err != nil {
				http.Error(w, "bad window", http.StatusBadRequest)
				return
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"calls": f.calls})
	})
	mux.HandleFunc("/api/v1/calls/", func(w http.ResponseWriter, r *http.Request) {
		rest := strings.TrimPrefix(r.URL.Path, "/api/v1/calls/")
		pkPath := strings.TrimSuffix(rest, "/trace")
		if status, ok := f.traceStatus[pkPath]; ok {
			w.WriteHeader(status)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})
	return mux
}

func callAt(ts time.Time, class string, idx int) model.CallJSON {
	return model.CallJSON{
		PK: model.PK{PodNamespace: "load", PodService: "svc", PodName: fmt.Sprintf("pod-%d", idx),
			RestartTimeMs: 1, TraceFileIndex: 0, BufferOffset: 0, RecordIndex: int32(idx)},
		TsMs:           ts.UnixMilli(),
		RetentionClass: class,
	}
}

func probeAgainst(t *testing.T, f *fakeQuery, cfg apiProbeConfig) *apiState {
	t.Helper()
	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)
	cfg.baseURL = srv.URL
	if cfg.freshnessBudget == 0 {
		cfg.freshnessBudget = 4 * time.Minute
	}
	if cfg.markerCount == 0 {
		cfg.markerCount = 10
	}
	if cfg.ttlMargin == 0 {
		cfg.ttlMargin = time.Minute
	}
	if cfg.classTTL == nil {
		cfg.classTTL = map[string]time.Duration{"short_clean": 15 * time.Minute}
	}
	return newAPIState(cfg)
}

func TestFreshness(t *testing.T) {
	now := time.Now()

	fresh := &fakeQuery{calls: []model.CallJSON{callAt(now.Add(-time.Minute), "short_clean", 1)}}
	a := probeAgainst(t, fresh, apiProbeConfig{})
	require.NoError(t, a.poll(context.Background(), now, false))
	assert.Empty(t, a.findings(nil), "a minute-old newest call is fresh")

	stale := &fakeQuery{calls: []model.CallJSON{callAt(now.Add(-10*time.Minute), "short_clean", 1)}}
	a = probeAgainst(t, stale, apiProbeConfig{})
	require.NoError(t, a.poll(context.Background(), now, false))
	require.Len(t, a.findings(nil), 1, "a 10-minute-old newest call breaks the 4-minute budget")
	assert.Contains(t, a.findings(nil)[0].msg, "newest call")

	empty := &fakeQuery{}
	a = probeAgainst(t, empty, apiProbeConfig{})
	require.NoError(t, a.poll(context.Background(), now, false))
	require.Len(t, a.findings(nil), 1)
	assert.Contains(t, a.findings(nil)[0].msg, "no calls")

	// A guard rejection of the small freshness window is a §8.7 violation,
	// not a transport gap: the UI cannot list recent calls.
	guarded := &fakeQuery{guardCalls: true}
	a = probeAgainst(t, guarded, apiProbeConfig{})
	require.NoError(t, a.poll(context.Background(), now, false))
	require.Len(t, a.findings(nil), 1)
	assert.Contains(t, a.findings(nil)[0].msg, "guard rejected")
}

func TestMarkersRetrievableUntilTTL(t *testing.T) {
	now := time.Now()
	young := callAt(now.Add(-2*time.Minute), "short_clean", 1)
	f := &fakeQuery{
		calls:       []model.CallJSON{callAt(now.Add(-time.Minute), "short_clean", 0), young},
		traceStatus: map[string]int{},
	}
	a := probeAgainst(t, f, apiProbeConfig{})

	// First post-warm-up poll samples the markers; traces answer 404.
	require.NoError(t, a.poll(context.Background(), now, true))
	require.True(t, a.markersSampled)
	var markerFindings []finding
	for _, fd := range a.findings(nil) {
		if strings.Contains(fd.msg, "trace answered") {
			markerFindings = append(markerFindings, fd)
		}
	}
	require.Len(t, markerFindings, 2, "a 404 for a young marker violates §8.7")

	// Serve the traces and the findings clear (the latch upstream keeps the
	// history; the predicate is pure).
	for _, c := range f.calls {
		f.traceStatus[c.PK.PathString()] = http.StatusOK
	}
	require.NoError(t, a.poll(context.Background(), now, true))
	for _, fd := range a.findings(nil) {
		assert.NotContains(t, fd.msg, "trace answered")
	}
}

func TestMarkersLeaveTheSetPastTTL(t *testing.T) {
	now := time.Now()
	expired := callAt(now.Add(-20*time.Minute), "short_clean", 1) // TTL is 15m
	f := &fakeQuery{calls: []model.CallJSON{expired}, traceStatus: map[string]int{}}
	a := probeAgainst(t, f, apiProbeConfig{})

	require.NoError(t, a.poll(context.Background(), now, true))
	assert.Empty(t, a.markers, "a marker past its TTL leaves the set without a violation")
}

func TestExpectTTLDeletion(t *testing.T) {
	now := time.Now()
	expired := callAt(now.Add(-20*time.Minute), "short_clean", 1) // TTL 15m, settle 2m
	f := &fakeQuery{
		calls:       []model.CallJSON{expired},
		traceStatus: map[string]int{expired.PK.PathString(): http.StatusOK},
	}
	a := probeAgainst(t, f, apiProbeConfig{expectTTLDeletion: true, ttlSettle: 2 * time.Minute})

	retrievable := func() []finding {
		var out []finding
		for _, fd := range a.findings(nil) {
			if strings.Contains(fd.msg, "still retrievable") {
				out = append(out, fd)
			}
		}
		return out
	}

	require.NoError(t, a.poll(context.Background(), now, true))
	require.Len(t, retrievable(), 1, "a 200 past TTL + settle violates -expect-ttl-deletion")
	assert.Len(t, a.markers, 1, "the marker stays watched until it disappears")

	// Once deleted for real, the marker leaves the set.
	f.traceStatus[expired.PK.PathString()] = http.StatusNotFound
	require.NoError(t, a.poll(context.Background(), now, true))
	assert.Empty(t, retrievable())
	assert.Empty(t, a.markers)
}

func TestMarkersSkipCorrupted(t *testing.T) {
	now := time.Now()
	f := &fakeQuery{calls: []model.CallJSON{
		callAt(now.Add(-time.Minute), model.RetentionCorrupted, 1),
		callAt(now.Add(-time.Minute), "short_clean", 2),
	}}
	a := probeAgainst(t, f, apiProbeConfig{})
	require.NoError(t, a.poll(context.Background(), now, true))
	require.Len(t, a.markers, 1, "the reserved corrupted class is never a marker")
	assert.Equal(t, "short_clean", a.markers[0].class)
}
