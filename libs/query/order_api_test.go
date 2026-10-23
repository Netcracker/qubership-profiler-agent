package query

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	storageparquet "github.com/Netcracker/qubership-profiler-backend/libs/storage/parquet"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// orderBaseMs is second-aligned: every planted file holds rows of one second,
// the precision of the sealed-key stamps.
const orderBaseMs = int64(1_700_000_000_000)

var orderTuple = model.PodTuple{Namespace: "ns", Service: "svc", Pod: "pod", RestartTimeMs: orderBaseMs}

// orderCall is one synthetic call: its class, its duration, and the buffer
// offset that makes its PK unique and orders the ties.
type orderCall struct {
	class string
	durMs int32
	off   int32
}

func (c orderCall) row(tier model.Tier) model.CallRow {
	return model.CallRow{
		PK: model.PK{PodNamespace: orderTuple.Namespace, PodService: orderTuple.Service, PodName: orderTuple.Pod,
			RestartTimeMs: orderTuple.RestartTimeMs, TraceFileIndex: 1, BufferOffset: c.off},
		TsMs: orderBaseMs + int64(c.off), DurationMs: c.durMs, Method: "com.example.Service.handle",
		ErrorFlag: c.class == model.RetentionAnyError, RetentionClass: c.class, Tier: tier,
	}
}

// plantCold writes one sealed file per call, each in its own second so the
// keys stay unique.
func plantCold(t *testing.T, store *memColdStore, calls ...orderCall) {
	t.Helper()
	for _, c := range calls {
		tsMs := orderBaseMs + int64(c.off)*1000
		var buf bytes.Buffer
		w := parquet.NewGenericWriter[storageparquet.CallV2](&buf,
			parquet.Compression(&parquet.Zstd),
			parquet.KeyValueMetadata(storageparquet.SchemaVersionKey, storageparquet.SchemaVersion))
		_, err := w.Write([]storageparquet.CallV2{{
			TsMs: tsMs, PodId: "ns/svc/pod", RestartTimeMs: orderTuple.RestartTimeMs,
			TraceFileIndex: 1, BufferOffset: c.off,
			Namespace: orderTuple.Namespace, ServiceName: orderTuple.Service, PodName: orderTuple.Pod,
			Method: "com.example.Service.handle", DurationMs: c.durMs,
			ErrorFlag: c.class == model.RetentionAnyError, RetentionClass: c.class,
		}})
		require.NoError(t, err)
		require.NoError(t, w.Close())
		store.put(sealedKey(c.class, orderTuple, tsMs), buf.Bytes())
	}
}

type callsPage struct {
	Calls      []model.CallJSON `json:"calls"`
	NextCursor *string          `json:"next_cursor"`
	Partial    bool             `json:"partial"`
	Reasons    []string         `json:"partial_reasons"`
}

func getCallsPage(t *testing.T, api string, params url.Values) callsPage {
	t.Helper()
	resp, body := getJSONBody(t, api+"/api/v1/calls?"+params.Encode())
	require.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	var p callsPage
	require.NoError(t, json.Unmarshal(body, &p))
	return p
}

// pageIDs pages a duration_desc query to the end and returns each page's
// call ids; between pages it runs the optional hook.
func pageIDs(t *testing.T, api string, params url.Values, between func(page int)) [][]string {
	t.Helper()
	var pages [][]string
	for i := 0; i < 50; i++ {
		p := getCallsPage(t, api, params)
		ids := []string{}
		for _, c := range p.Calls {
			ids = append(ids, fmt.Sprintf("%d/%d", c.DurationMs, c.PK.BufferOffset))
		}
		pages = append(pages, ids)
		if p.NextCursor == nil {
			return pages
		}
		if between != nil {
			between(i)
		}
		params = url.Values{"cursor": {*p.NextCursor}, "limit": params["limit"]}
	}
	t.Fatal("paging did not end")
	return nil
}

func durationQuery(limit int) url.Values {
	return url.Values{
		"from": {strconv.FormatInt(orderBaseMs, 10)}, "to": {strconv.FormatInt(orderBaseMs+3_600_000, 10)},
		"order": {"duration_desc"}, "limit": {strconv.Itoa(limit)},
	}
}

// bandCalls cover every band, with an error call tied at the long_clean lower
// bound between two long_clean calls.
var bandCalls = []orderCall{
	{model.RetentionHugeClean, 20_000, 1}, {model.RetentionHugeClean, 15_000, 2},
	{model.RetentionAnyError, 12_000, 3},
	{model.RetentionLongClean, 5000, 4},
	{model.RetentionLongClean, 1000, 10}, {model.RetentionAnyError, 1000, 20}, {model.RetentionLongClean, 1000, 30},
	{model.RetentionNormalClean, 999, 5}, {model.RetentionNormalClean, 100, 6},
	{model.RetentionShortClean, 99, 7}, {model.RetentionAnyError, 50, 8}, {model.RetentionShortClean, 5, 9},
}

func bandOrder() []string {
	return []string{"20000/1", "15000/2", "12000/3", "5000/4", "1000/10", "1000/20", "1000/30",
		"999/5", "100/6", "99/7", "50/8", "5/9"}
}

func TestDurationOrderWalksTheBands(t *testing.T) {
	store := newMemColdStore()
	plantCold(t, store, bandCalls...)
	api := httptest.NewServer(New(Options{ColdStore: store}).Handler())
	defer api.Close()

	first := getCallsPage(t, api.URL, durationQuery(3))
	assert.Equal(t, []string{model.RetentionAnyError, model.RetentionCorrupted, model.RetentionHugeClean},
		store.listedClasses(), "a full first page opens the longest band and the error classes only")
	require.NotNil(t, first.NextCursor)
	tok, err := decodeCursor(*first.NextCursor, time.Minute)
	require.NoError(t, err)
	assert.Equal(t, model.RetentionHugeClean, tok.Band)

	assert.Equal(t, slices.Concat(pageIDs(t, api.URL, durationQuery(3), nil)...), bandOrder(),
		"pages concatenate into (duration_ms DESC, pk ASC), ties at a band bound included")

	t.Run("conditions skip bands", func(t *testing.T) {
		q := durationQuery(1)
		q.Set("filter", "duration<1s")
		q["retention_class"] = []string{model.RetentionNormalClean, model.RetentionShortClean}
		assert.Equal(t, []string{"999/5", "100/6", "99/7", "5/9"}, slices.Concat(pageIDs(t, api.URL, q, nil)...))
	})
}

func TestDurationOrderStopsAtABandTheGuardRejects(t *testing.T) {
	store := newMemColdStore()
	plantCold(t, store, bandCalls...)
	// The three error files plus any one band fit; two bands never do.
	cfg := Config{MaxScanFiles: 6}
	api := httptest.NewServer(New(Options{Config: cfg, ColdStore: store}).Handler())
	defer api.Close()

	pages := pageIDs(t, api.URL, durationQuery(3), nil)
	assert.Equal(t, bandOrder(), slices.Concat(pages...), "stopped pages lose and repeat nothing")
	assert.Contains(t, pages, []string{}, "a page stopped before any final row is empty but not the end")

	t.Run("a stop keeps the rows above the bound and resumes below it", func(t *testing.T) {
		after := model.Position{DurationMs: 12_000, PK: bandCalls[2].row(model.TierCold).PK}
		params := url.Values{"cursor": {encodeCursor(durationQuery(3), after, model.RetentionHugeClean)}, "limit": {"3"}}
		p := getCallsPage(t, api.URL, params)
		assert.Empty(t, p.Calls, "the long band did not fit, and no row past the cursor is at or above 10s")
		require.NotNil(t, p.NextCursor)
		tok, err := decodeCursor(*p.NextCursor, time.Minute)
		require.NoError(t, err)
		assert.Equal(t, model.Position{DurationMs: 10_000, NoPK: true}, tok.Pos)
		assert.Equal(t, model.RetentionLongClean, tok.Band, "the next page starts at the band that did not fit")
	})
}

func TestDurationOrderGuard(t *testing.T) {
	store := newMemColdStore()
	plantCold(t, store, bandCalls...)
	api := httptest.NewServer(New(Options{Config: Config{MaxScanFiles: 2}, ColdStore: store}).Handler())
	defer api.Close()

	requestProblem(t, api, http.MethodGet, "/api/v1/calls?"+durationQuery(3).Encode(),
		http.StatusBadRequest, "query_too_wide")
	_, raw := getJSONBody(t, api.URL+"/api/v1/calls?"+durationQuery(3).Encode())
	var body struct {
		ByClass map[string]int64 `json:"by_class"`
	}
	require.NoError(t, json.Unmarshal(raw, &body))
	assert.ElementsMatch(t, []string{model.RetentionHugeClean, model.RetentionAnyError}, slices.Collect(maps.Keys(body.ByClass)),
		"the estimate names the classes of the first band and nothing the page would not open")

	wide := durationQuery(3)
	wide.Set("to", strconv.FormatInt(orderBaseMs+7*3_600_000, 10))
	open := httptest.NewServer(New(Options{ColdStore: store}).Handler())
	defer open.Close()
	assert.Len(t, getCallsPage(t, open.URL, wide).Calls, 3,
		"duration_desc counts as a narrowing filter for the span layer (02 §2.3.5)")
	wide.Del("order")
	requestProblem(t, open, http.MethodGet, "/api/v1/calls?"+wide.Encode(), http.StatusBadRequest, "query_too_wide")
}

func TestOrderRejections(t *testing.T) {
	api := httptest.NewServer(New(Options{ColdStore: newMemColdStore()}).Handler())
	defer api.Close()
	q := durationQuery(3)
	q.Set("order", "cpu_desc")
	p := requestProblem(t, api, http.MethodGet, "/api/v1/calls?"+q.Encode(), http.StatusBadRequest, "invalid_request")
	assert.Equal(t, "order cpu_desc is not implemented", p.Detail)

	cur := encodeCursor(durationQuery(3), model.Position{DurationMs: 50}, model.RetentionShortClean)
	resent := url.Values{"cursor": {cur}, "order": {"start_desc"}}
	requestProblem(t, api, http.MethodGet, "/api/v1/calls?"+resent.Encode(), http.StatusBadRequest, "cursor_rejected")
}

// durationReplica is a collector that implements duration_desc over a
// mutable call list, so a test can migrate a call to the cold tier between
// pages. Without echo it plays an older build that ignores `order`.
type durationReplica struct {
	mu       sync.Mutex
	calls    []orderCall
	oldestMs int64
	noEcho   bool
}

func (r *durationReplica) remove(off int32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = slices.DeleteFunc(r.calls, func(c orderCall) bool { return c.off == off })
}

func (r *durationReplica) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.mu.Lock()
	defer r.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	switch req.URL.Path {
	case "/internal/v1/health/hot-window":
		_ = json.NewEncoder(w).Encode(map[string]int64{"hot_window_oldest_ms": r.oldestMs})
	case "/internal/v1/calls":
		q := req.URL.Query()
		limit, _ := strconv.Atoi(q.Get("limit"))
		var after *model.Position
		if raw := q.Get("after_duration_ms"); raw != "" {
			d, _ := strconv.Atoi(raw)
			after = &model.Position{DurationMs: int32(d), NoPK: true}
			if pkRaw := q.Get("after_pk"); pkRaw != "" {
				pk, err := model.ParsePKPath(pkRaw)
				if err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				after.PK, after.NoPK = pk, false
			}
		}
		var rows []model.CallRow
		for _, c := range r.calls {
			row := c.row(model.TierHot)
			if after == nil || model.OrderDurationDesc.Past(row.Position(), *after) {
				rows = append(rows, row)
			}
		}
		model.OrderDurationDesc.SortRows(rows)
		calls := []model.CallJSON{}
		for _, row := range rows[:min(limit, len(rows))] {
			calls = append(calls, row.JSON())
		}
		body := map[string]any{"calls": calls, "partial_reasons": []string{}}
		if !r.noEcho {
			body["order"] = q.Get("order")
		}
		_ = json.NewEncoder(w).Encode(body)
	default:
		http.NotFound(w, req)
	}
}

// TestDurationOrderAcrossTheHotToColdMigration moves a call from the hot tier
// to the cold tier between two pages, and keeps a duplicate in both tiers:
// the merge places each call once, in order, whichever tier holds it.
func TestDurationOrderAcrossTheHotToColdMigration(t *testing.T) {
	store := newMemColdStore()
	plantCold(t, store,
		orderCall{model.RetentionLongClean, 4000, 2},
		orderCall{model.RetentionLongClean, 3000, 3}, // also hot: an overlap-window duplicate
	)
	replica := &durationReplica{
		// The hot window starts after the cold files, and the overlap margin
		// keeps the cold tier in the query.
		oldestMs: orderBaseMs + 10_000,
		calls: []orderCall{
			{model.RetentionLongClean, 5000, 1}, {model.RetentionLongClean, 3000, 3},
			{model.RetentionLongClean, 2000, 4}, {model.RetentionNormalClean, 500, 5},
		},
	}
	hotSrv := httptest.NewServer(replica)
	defer hotSrv.Close()
	api := httptest.NewServer(New(Options{ColdStore: store, HotDiscovery: staticDiscovery{hotSrv.URL}}).Handler())
	defer api.Close()

	pages := pageIDs(t, api.URL, durationQuery(2), func(page int) {
		if page == 0 {
			replica.remove(4)
			plantCold(t, store, orderCall{model.RetentionLongClean, 2000, 4})
		}
	})
	assert.Equal(t, []string{"5000/1", "4000/2", "3000/3", "2000/4", "500/5"}, slices.Concat(pages...))
	assert.Equal(t, []string{"5000/1", "4000/2"}, pages[0], "hot and cold rows merge by one comparator")
}

// TestDurationOrderOverAnOlderReplica: a collector that ignores `order`
// answers its newest rows, which cannot stand in for its longest. Its rows are
// dropped and the page reports the replica as a partial source.
func TestDurationOrderOverAnOlderReplica(t *testing.T) {
	current := &durationReplica{calls: []orderCall{{model.RetentionLongClean, 5000, 1}}}
	older := &durationReplica{noEcho: true, calls: []orderCall{{model.RetentionHugeClean, 50_000, 2}}}
	srvA, srvB := httptest.NewServer(current), httptest.NewServer(older)
	defer srvA.Close()
	defer srvB.Close()
	api := httptest.NewServer(New(Options{HotDiscovery: staticDiscovery{srvA.URL, srvB.URL}}).Handler())
	defer api.Close()

	p := getCallsPage(t, api.URL, durationQuery(10))
	require.Len(t, p.Calls, 1)
	assert.EqualValues(t, 5000, p.Calls[0].DurationMs)
	assert.True(t, p.Partial)
	require.Len(t, p.Reasons, 1)
	assert.Contains(t, p.Reasons[0], "order=duration_desc")
}
