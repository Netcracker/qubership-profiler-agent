package query

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/httpproblem"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/cold"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/stats"
	storageparquet "github.com/Netcracker/qubership-profiler-backend/libs/storage/parquet"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// statsT is an hour-aligned base time, so every fixture file lands under one
// hour prefix of the window.
const statsT = int64(1_699_999_200_000)

// statsCall is one synthetic call. off makes the PK unique within a pod.
func statsCall(service string, off int32, tsOffMs int64, durationMs int32, isError bool) model.CallRow {
	return model.CallRow{
		PK: model.PK{PodNamespace: "ns", PodService: service, PodName: service + "-0",
			RestartTimeMs: statsT - 3_600_000, TraceFileIndex: 1, BufferOffset: off},
		TsMs: statsT + tsOffMs, DurationMs: durationMs, Method: "com.example." + service + ".handle",
		ErrorFlag: isError, CpuTimeMs: int64(durationMs) / 2, RetentionClass: model.RetentionNormalClean,
	}
}

// writeStatsParquet renders rows into one parquet object in the file order
// (ts_ms DESC, pk ASC), with optional extra footer metadata.
func writeStatsParquet(t *testing.T, rows []model.CallRow, meta ...parquet.WriterOption) []byte {
	t.Helper()
	rows = append([]model.CallRow(nil), rows...)
	sort.Slice(rows, func(i, j int) bool { return rows[i].Position().Before(rows[j].Position()) })
	var buf bytes.Buffer
	opts := append([]parquet.WriterOption{parquet.Compression(&parquet.Zstd),
		parquet.KeyValueMetadata(storageparquet.SchemaVersionKey, storageparquet.SchemaVersion)}, meta...)
	w := parquet.NewGenericWriter[storageparquet.CallV2](&buf, opts...)
	for _, r := range rows {
		_, err := w.Write([]storageparquet.CallV2{{
			TsMs: r.TsMs, PodId: r.PK.PodNamespace + "/" + r.PK.PodService + "/" + r.PK.PodName,
			RestartTimeMs: r.PK.RestartTimeMs, TraceFileIndex: r.PK.TraceFileIndex,
			BufferOffset: r.PK.BufferOffset, RecordIndex: r.PK.RecordIndex,
			Namespace: r.PK.PodNamespace, ServiceName: r.PK.PodService, PodName: r.PK.PodName,
			Method: r.Method, DurationMs: r.DurationMs, CpuTimeMs: r.CpuTimeMs,
			ErrorFlag: r.ErrorFlag, RetentionClass: r.RetentionClass,
		}})
		require.NoError(t, err)
	}
	require.NoError(t, w.Close())
	return buf.Bytes()
}

// coldObjectKey builds a 01 §7 key whose time bounds cover rows.
func coldObjectKey(replica, hash string, seq int, rows []model.CallRow) string {
	const stamp = "20060102T150405Z"
	lo, hi := rows[0].TsMs, rows[0].TsMs
	for _, r := range rows {
		lo, hi = min(lo, r.TsMs), max(hi, r.TsMs)
	}
	minT, maxT := time.UnixMilli(lo).UTC(), time.UnixMilli(hi).UTC()
	bucket := minT.Truncate(5 * time.Minute)
	name := strings.Join([]string{replica, hash, bucket.Format(stamp), minT.Format(stamp), maxT.Format(stamp),
		strconv.Itoa(seq)}, "-") + ".parquet"
	return path.Join("parquet/v1", model.RetentionNormalClean, bucket.Format("2006/01/02/15"), name)
}

// putCold seals rows into one object of a collector and returns its key.
func putCold(t *testing.T, store *memColdStore, seq int, rows ...model.CallRow) string {
	t.Helper()
	key := coldObjectKey("collector-0", "abc", seq, rows)
	store.put(key, writeStatsParquet(t, rows))
	return key
}

// statsReplica is a fake collector: a hot window and /internal/v1/stats over
// its rows, aggregated with the same code the real collector runs.
type statsReplica struct {
	oldestMs  int64
	rows      []model.CallRow
	failProbe bool
	failStats bool
	// extraHashes pads pk_hashes, to make the hash set outgrow the budget.
	extraHashes int
}

func (r *statsReplica) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	switch {
	case strings.HasSuffix(req.URL.Path, "/health/hot-window"):
		if r.failProbe {
			http.Error(w, "down", http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]int64{"hot_window_oldest_ms": r.oldestMs, "hot_window_now_ms": r.oldestMs})
	case strings.HasSuffix(req.URL.Path, "/internal/v1/stats"):
		if r.failStats {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		in, detail := stats.ParseInternalRequest(req.URL.Query())
		if detail != "" {
			http.Error(w, detail, http.StatusBadRequest)
			return
		}
		match, err := in.Query.Matcher()
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		agg := stats.NewAggregator(in.Grouping, in.Order, in.MaxGroups, nil)
		var hashes []uint64
		for i := range r.rows {
			row := r.rows[i]
			if !match.Match(&row) {
				continue
			}
			hashes = append(hashes, stats.PKHash(row.PK))
			_ = agg.Add(&row)
		}
		for i := 0; i < r.extraHashes; i++ {
			hashes = append(hashes, uint64(i)<<20|1)
		}
		resp := stats.InternalResponse{GroupBy: in.Grouping.Field, Totals: agg.Totals(),
			Approximate: agg.Approximate(), PKHashes: stats.EncodeHashes(hashes)}
		for _, g := range agg.Groups() {
			resp.Groups = append(resp.Groups, stats.InternalGroupOf(g))
		}
		_ = json.NewEncoder(w).Encode(resp)
	default:
		http.NotFound(w, req)
	}
}

type statsFixture struct {
	cold     *memColdStore
	replicas []*statsReplica
	cfg      Config
}

func (f *statsFixture) serve(t *testing.T) *httptest.Server {
	t.Helper()
	opts := Options{Config: f.cfg, ColdStore: f.cold}
	if f.replicas != nil {
		var urls staticDiscovery
		for _, r := range f.replicas {
			srv := httptest.NewServer(r)
			t.Cleanup(srv.Close)
			urls = append(urls, srv.URL)
		}
		opts.HotDiscovery = urls
	}
	api := httptest.NewServer(New(opts).Handler())
	t.Cleanup(api.Close)
	return api
}

func getStats(t *testing.T, api *httptest.Server, kv ...string) stats.Response {
	t.Helper()
	v := url.Values{"from": {strconv.FormatInt(statsT, 10)}, "to": {strconv.FormatInt(statsT+100_000, 10)}}
	for i := 0; i < len(kv); i += 2 {
		v.Add(kv[i], kv[i+1])
	}
	resp, body := getJSONBody(t, api.URL+"/api/v1/stats?"+v.Encode())
	require.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	var out stats.Response
	require.NoError(t, json.Unmarshal(body, &out))
	return out
}

func rowsByKey(resp stats.Response) map[string]stats.RowValues {
	out := map[string]stats.RowValues{}
	for _, r := range resp.Rows {
		out[r.Key.(string)] = r.RowValues
	}
	return out
}

// tieredFixture is the known data set across both tiers:
//
//   - five old calls only cold holds, below every hot window;
//   - replica A (oldest T+50s) holds three calls, two of them also sealed to
//     cold (the overlap) and one hot-only;
//   - replica B (oldest T+70s) holds one hot-only call, and one call it has
//     already dropped lives in cold only, inside [t_lo, to).
//
// Exact answer: 10 calls, every one counted once.
func tieredFixture(t *testing.T) *statsFixture {
	t.Helper()
	store := newMemColdStore()
	old := []model.CallRow{
		statsCall("billing", 1, 1_000, 10, false),
		statsCall("billing", 2, 2_000, 20, true),
		statsCall("billing", 3, 3_000, 30, false),
		statsCall("orders", 4, 4_000, 40, false),
		statsCall("orders", 5, 5_000, 50, false),
	}
	putCold(t, store, 0, old...)
	overlap := []model.CallRow{
		statsCall("billing", 10, 50_000, 100, false),
		statsCall("billing", 11, 55_000, 200, false),
	}
	putCold(t, store, 1, overlap...)
	droppedByB := statsCall("orders", 20, 65_000, 300, true)
	putCold(t, store, 2, droppedByB)

	a := &statsReplica{oldestMs: statsT + 50_000,
		rows: append(append([]model.CallRow{}, overlap...), statsCall("billing", 12, 60_000, 400, false))}
	b := &statsReplica{oldestMs: statsT + 70_000, rows: []model.CallRow{statsCall("orders", 21, 80_000, 500, false)}}
	return &statsFixture{cold: store, replicas: []*statsReplica{a, b}}
}

func TestStatsCountsEachCallOnce(t *testing.T) {
	api := tieredFixture(t).serve(t)

	resp := getStats(t, api, "group_by", "service")
	assert.Equal(t, stats.Totals{Count: 10, Errors: 2}, resp.Totals)
	assert.False(t, resp.Partial, "reasons: %v", resp.PartialReasons)
	assert.Equal(t, []string{}, resp.PartialReasons)
	byKey := rowsByKey(resp)
	assert.Equal(t, int64(6), byKey["billing"].Count, "three old, two in both tiers, one hot-only")
	assert.Equal(t, int64(10+20+30+100+200+400), byKey["billing"].DurationMsSum)
	assert.Equal(t, int64(4), byKey["orders"].Count, "two old, one cold-only in [t_lo, to), one hot-only")
	assert.Equal(t, int64(1), byKey["orders"].Errors)
	assert.Equal(t, int64(40+50+300+500), byKey["orders"].DurationMsSum)
	assert.Equal(t, 2, resp.Distinct)
	assert.Nil(t, resp.Other)
	assert.False(t, resp.Approximate)
	assert.Equal(t, "billing", resp.Rows[0].Key, "count_desc")

	t.Run("percentiles over both tiers", func(t *testing.T) {
		// billing durations: 10 20 30 100 200 400; rank floor(0.5·5) = 2 → 30.
		assert.InDelta(t, 30, *byKey["billing"].P50Ms, 0.3)
		// rank floor(0.99·5) = 4 → 200: the top value needs n > 100 to be p99.
		assert.InDelta(t, 200, *byKey["billing"].P99Ms, 2)
	})
	t.Run("time buckets across the tiers", func(t *testing.T) {
		resp := getStats(t, api, "group_by", "time", "buckets", "3")
		require.NotNil(t, resp.BucketMs)
		assert.Equal(t, int64(33_334), *resp.BucketMs)
		require.Len(t, resp.Rows, 3)
		assert.Equal(t, float64(statsT+66_668), resp.Rows[2].Key, "the last bucket is the short one")
		assert.Equal(t, []int64{5, 4, 1}, []int64{resp.Rows[0].Count, resp.Rows[1].Count, resp.Rows[2].Count})
		assert.Equal(t, int64(10), resp.Totals.Count)
	})
	t.Run("conditions apply to both tiers", func(t *testing.T) {
		resp := getStats(t, api, "group_by", "method", "filter", "duration>=200ms")
		assert.Equal(t, int64(4), resp.Totals.Count, "200, 300, 400, 500")
	})
	t.Run("limit, other, and duration order", func(t *testing.T) {
		resp := getStats(t, api, "group_by", "service", "limit", "1", "order", "duration_sum_desc")
		require.Len(t, resp.Rows, 1)
		assert.Equal(t, "orders", resp.Rows[0].Key, "890 ms beats 760 ms")
		require.NotNil(t, resp.Other)
		assert.Equal(t, int64(6), resp.Other.Count)
		assert.Equal(t, 2, resp.Distinct)
	})
}

// TestStatsEmptyReplicaKeepsHotOnlyCalls is the review's failing case: an
// empty replica reports oldest = now. Taking the latest hot window as the
// split would move it past replica A's hot-only call and drop it; t_lo is
// the earliest window, so the empty replica moves nothing.
func TestStatsEmptyReplicaKeepsHotOnlyCalls(t *testing.T) {
	f := tieredFixture(t)
	f.replicas = append(f.replicas, &statsReplica{oldestMs: statsT + 10*3_600_000})
	resp := getStats(t, f.serve(t), "group_by", "service")
	assert.Equal(t, int64(10), resp.Totals.Count)
	assert.False(t, resp.Partial)
}

func TestStatsSkipsCompactionInputs(t *testing.T) {
	store := newMemColdStore()
	rows := []model.CallRow{statsCall("billing", 1, 1_000, 10, false), statsCall("billing", 2, 2_000, 20, false)}
	more := []model.CallRow{statsCall("billing", 3, 3_000, 30, false)}
	in1 := putCold(t, store, 0, rows...)
	in2 := putCold(t, store, 1, more...)
	// A first compaction merged in1 and in2; a second one took its output as
	// input. Every object is still listed within the delete grace.
	all := append(append([]model.CallRow{}, rows...), more...)
	m1 := coldObjectKey(cold.MaintainReplica, "1111", 0, all)
	store.put(m1, writeStatsParquet(t, all,
		parquet.KeyValueMetadata(storageparquet.CompactionInputsKey, in1+"\n"+in2)))
	m2 := coldObjectKey(cold.MaintainReplica, "2222", 0, all)
	store.put(m2, writeStatsParquet(t, all, parquet.KeyValueMetadata(storageparquet.CompactionInputsKey, m1)))

	api := (&statsFixture{cold: store}).serve(t)
	resp := getStats(t, api, "group_by", "service")
	assert.Equal(t, int64(3), resp.Totals.Count, "only the newest compacted object is read")
	assert.Equal(t, int64(60), rowsByKey(resp)["billing"].DurationMsSum)
}

func TestStatsDegradedHotTier(t *testing.T) {
	t.Run("every probe failed: cold covers the window, partial", func(t *testing.T) {
		f := tieredFixture(t)
		for _, r := range f.replicas {
			r.failProbe = true
		}
		resp := getStats(t, f.serve(t), "group_by", "service")
		assert.Equal(t, int64(8), resp.Totals.Count, "every cold call, no hot-only one")
		assert.True(t, resp.Partial)
		assert.NotEmpty(t, resp.PartialReasons)
	})
	t.Run("one failed stats call leaves that replica out", func(t *testing.T) {
		f := tieredFixture(t)
		f.replicas[0].failStats = true
		resp := getStats(t, f.serve(t), "group_by", "service")
		assert.Equal(t, int64(9), resp.Totals.Count, "A's overlap calls still come from cold; its hot-only one is lost")
		assert.True(t, resp.Partial)
		require.Len(t, resp.PartialReasons, 1)
		assert.Contains(t, resp.PartialReasons[0], "stats")
	})
	t.Run("the hash sets outgrow the budget", func(t *testing.T) {
		f := tieredFixture(t)
		f.cfg = Config{ReadMemoryBudget: 1 << 20, ReadBudgetWait: 20 * time.Millisecond}
		f.replicas[0].extraHashes = 200_000 // 1.6 MB of hashes, 2.1 MB of base64
		resp := getStats(t, f.serve(t), "group_by", "service")
		assert.Equal(t, int64(8), resp.Totals.Count, "the hot side is dropped; cold covers the window")
		assert.True(t, resp.Partial)
		assert.Contains(t, resp.PartialReasons, reasonReadBudgetExhausted)
	})
	t.Run("no replica found", func(t *testing.T) {
		f := tieredFixture(t)
		f.replicas = []*statsReplica{}
		resp := getStats(t, f.serve(t), "group_by", "service")
		assert.Equal(t, int64(8), resp.Totals.Count)
		assert.True(t, resp.Partial)
	})
	t.Run("a cold-only deployment is complete", func(t *testing.T) {
		f := tieredFixture(t)
		f.replicas = nil
		resp := getStats(t, f.serve(t), "group_by", "service")
		assert.Equal(t, int64(8), resp.Totals.Count)
		assert.False(t, resp.Partial)
	})
}

func TestStatsSpaceSavingIsApproximate(t *testing.T) {
	store := newMemColdStore()
	putCold(t, store, 0,
		statsCall("a", 1, 1_000, 10, false), statsCall("a", 2, 2_000, 10, false),
		statsCall("b", 3, 3_000, 10, false), statsCall("c", 4, 4_000, 10, false))
	api := (&statsFixture{cold: store, cfg: Config{StatsMaxGroups: 2}}).serve(t)
	resp := getStats(t, api, "group_by", "service")
	assert.True(t, resp.Approximate)
	assert.Equal(t, int64(4), resp.Totals.Count, "totals stay exact")
	assert.Equal(t, "a", resp.Rows[0].Key)
	assert.Equal(t, 2, resp.Distinct, "a lower bound once approximate")
}

func TestStatsRejections(t *testing.T) {
	api := (&statsFixture{cold: newMemColdStore()}).serve(t)
	base := url.Values{"from": {strconv.FormatInt(statsT, 10)}, "to": {strconv.FormatInt(statsT+100_000, 10)}}
	for name, tc := range map[string]struct {
		extra  url.Values
		code   string
		detail string
	}{
		"missing group_by":      {url.Values{}, httpproblem.CodeInvalidRequest, "group_by is required"},
		"no grouping":           {url.Values{"group_by": {"duration"}}, httpproblem.CodeInvalidRequest, "group_by=duration is not a grouping"},
		"buckets out of range":  {url.Values{"group_by": {"time"}, "buckets": {"1001"}}, httpproblem.CodeInvalidRequest, "buckets"},
		"limit on time":         {url.Values{"group_by": {"time"}, "limit": {"5"}}, httpproblem.CodeInvalidRequest, "limit applies"},
		"buckets on a field":    {url.Values{"group_by": {"pod"}, "buckets": {"5"}}, httpproblem.CodeInvalidRequest, "buckets applies"},
		"unknown order":         {url.Values{"group_by": {"pod"}, "order": {"duration_desc"}}, httpproblem.CodeInvalidRequest, "order must be"},
		"wide window, no prune": {url.Values{"group_by": {"pod"}, "to": {strconv.FormatInt(statsT+7*3_600_000, 10)}}, httpproblem.CodeQueryTooWide, "PROFILER_WIDE_RANGE_LIMIT"},
	} {
		t.Run(name, func(t *testing.T) {
			v := url.Values{}
			for k, vs := range base {
				v[k] = vs
			}
			for k, vs := range tc.extra {
				v[k] = vs
			}
			resp, body := getJSONBody(t, api.URL+"/api/v1/stats?"+v.Encode())
			require.Equal(t, http.StatusBadRequest, resp.StatusCode, "body: %s", body)
			var p struct {
				Code   string `json:"code"`
				Detail string `json:"detail"`
			}
			require.NoError(t, json.Unmarshal(body, &p))
			assert.Equal(t, tc.code, p.Code)
			assert.Contains(t, p.Detail, tc.detail)
		})
	}
}
