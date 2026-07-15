package integration

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/collector/hotread"
	"github.com/Netcracker/qubership-profiler-backend/libs/collector/hotstore"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	model "github.com/Netcracker/qubership-profiler-backend/libs/protocol"
	"github.com/Netcracker/qubership-profiler-backend/libs/query"
	storageparquet "github.com/Netcracker/qubership-profiler-backend/libs/storage/parquet"
	"github.com/Netcracker/qubership-profiler-backend/libs/tests/helpers/wire"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	fanPodHot  = "pod-fan-hot"
	fanPodCold = "pod-fan-cold"
)

// scriptedDiscovery is the test's replica set: the migration subtest empties
// it to simulate a replica whose data has fully aged into the cold tier.
type scriptedDiscovery struct {
	mu   sync.Mutex
	urls []string
}

func (d *scriptedDiscovery) Replicas(context.Context) ([]string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.urls...), nil
}

func (d *scriptedDiscovery) set(urls ...string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.urls = urls
}

// TestHotColdFanout drives the Stage 1 hot-fan-out slice acceptance
// (02-read-contract.md §3, §4.3, §2.3.1, §2.7, §6.3): a real collector serves
// its un-sealed hot store over /internal/v1, a second pod's data lives only
// in the in-test S3, and the query service fans out, computes the dynamic
// cutoff, merges the tiers, and dedups with cold preferred.
func TestHotColdFanout(t *testing.T) {
	ctx, cancel := context.WithCancel(log.SetLevel(context.Background(), log.INFO))
	defer cancel()
	fake := newColdFakeStore()

	// --- Phase 1: pod C on a collector that then disappears (scale-down).
	// Its sealed data and manifests survive only in S3: a closed, cold-only
	// pod-restart for the /pods union and the cold half of the merge.
	ctxB, cancelB := context.WithCancel(ctx)
	dirB := t.TempDir()
	svcB := startCollector(t, ctxB, dirB)
	storeB := svcB.Store()
	bucketOld := storeB.Config().Bucket(baseMs + 5)

	fileC, offC := wire.TraceStream(timerStartMs, []wire.TraceChunk{
		{ThreadId: 61, StartMs: baseMs + 40, Events: []wire.TraceEvent{
			wire.Enter(0, sealMethodQuery), wire.Exit(1),
		}},
		{ThreadId: 62, StartMs: baseMs + 45, Events: []wire.TraceEvent{
			wire.Enter(0, sealMethodProcess), wire.Exit(2),
		}},
	})
	callsC := []wire.CallRecord{
		{DeltaMs: 40, Method: sealMethodQuery, DurationMs: 30, ThreadName: "exec-1", // C1: short_clean
			TraceFileIndex: 1, BufferOffset: int(offC[0]), RecordIndex: 0},
		{DeltaMs: 5, Method: sealMethodProcess, DurationMs: 1500, ThreadName: "exec-2", // C2: long_clean
			TraceFileIndex: 1, BufferOffset: int(offC[1]), RecordIndex: 0},
	}
	acC := connectAgentAs(t, ctx, fanPodCold)
	keyC := waitForPodNamed(t, storeB, fanPodCold, 1)
	prC, ok := storeB.PodRestart(keyC)
	require.True(t, ok)
	sendStream(t, acC, model.StreamDictionary, 0, wire.DictionaryStream(sealDictWords))
	sendStream(t, acC, model.StreamTrace, 0, fileC)
	sendStream(t, acC, model.StreamCalls, 0, wire.CallsStreamRecords(baseMs, callsC))
	waitForIndexedCalls(t, storeB, bucketOld, keyC, 2)
	require.NoError(t, acC.CommandClose())
	_ = acC.Close()
	require.Eventually(t, prC.Finalized, 5*time.Second, 10*time.Millisecond)

	_, err := storeB.Seal(ctx, keyC, bucketOld)
	require.NoError(t, err)
	_, err = hotstore.NewUploader(storeB, fake).Pass(ctx)
	require.NoError(t, err)
	cancelB()
	waitForCollectorStop(t, svcB)

	// --- Phase 2: the live collector with pod H. The old bucket is sealed
	// AND uploaded, so its rows sit in both tiers (the §4.3 overlap); the new
	// bucket two hours later stays un-sealed: hot-only rows.
	hotMs := baseMs + 2*60*60*1000
	svcA := startCollector(t, ctx, t.TempDir())
	storeA := svcA.Store()
	bucketNew := storeA.Config().Bucket(hotMs + 5)

	fileH1, offH1 := wire.TraceStream(timerStartMs, []wire.TraceChunk{
		{ThreadId: 11, StartMs: baseMs + 5, Events: []wire.TraceEvent{
			wire.Enter(0, sealMethodHandle), wire.Tag(1, sealDictRequestId, "req-1"), wire.Exit(2),
		}},
		{ThreadId: 22, StartMs: baseMs + 10, Events: []wire.TraceEvent{
			wire.Enter(0, sealMethodQuery), wire.Exit(3),
		}},
	})
	fileH2, offH2 := wire.TraceStream(timerStartMs, []wire.TraceChunk{
		{ThreadId: 33, StartMs: hotMs + 5, Events: []wire.TraceEvent{
			wire.Enter(0, sealMethodProcess), wire.Exit(4),
		}},
		{ThreadId: 44, StartMs: hotMs + 10, Events: []wire.TraceEvent{
			wire.Enter(0, sealMethodHandle), wire.Exit(1),
		}},
	})
	callsH := []wire.CallRecord{
		{DeltaMs: 5, Method: sealMethodHandle, DurationMs: 10, ThreadName: "exec-1", // A1: short_clean
			TraceFileIndex: 1, BufferOffset: int(offH1[0]), RecordIndex: 0,
			Params: map[int][]string{sealDictRequestId: {"req-1"}}},
		{DeltaMs: 5, Method: sealMethodQuery, DurationMs: 700, ThreadName: "exec-2", // E: errored
			TraceFileIndex: 1, BufferOffset: int(offH1[1]), RecordIndex: 0,
			Params: map[int][]string{sealDictCallRed: {"1"}}},
		{DeltaMs: 2*60*60*1000 - 5, Method: sealMethodProcess, DurationMs: 2000, ThreadName: "exec-3", // N1: long_clean
			TraceFileIndex: 2, BufferOffset: int(offH2[0]), RecordIndex: 0},
		{DeltaMs: 5, Method: sealMethodHandle, DurationMs: 50, ThreadName: "exec-4", // N2: short_clean
			TraceFileIndex: 2, BufferOffset: int(offH2[1]), RecordIndex: 0},
	}

	acH := connectAgentAs(t, ctx, fanPodHot)
	keyH := waitForPodNamed(t, storeA, fanPodHot, 1)
	prH, ok := storeA.PodRestart(keyH)
	require.True(t, ok)
	// Production-like ordering: the calls stream — including the errored call
	// E — lands before the dictionary decodes "call.red", so the hot index
	// provably stores the provisional error_flag = false. The seal re-derives
	// it (01 §5.6); §6.3 cold-preferred dedup is then observable.
	sendStream(t, acH, model.StreamTrace, 0, fileH1)
	sendStream(t, acH, model.StreamTrace, 1, fileH2)
	sendStream(t, acH, model.StreamCalls, 0, wire.CallsStreamRecords(baseMs, callsH))
	waitForIndexedCalls(t, storeA, bucketOld, keyH, 2)
	waitForIndexedCalls(t, storeA, bucketNew, keyH, 2)
	for _, row := range mustCalls(t, storeA, bucketOld, keyH) {
		if row.TraceFileIndex == 1 && row.BufferOffset == int(offH1[1]) {
			require.False(t, row.ErrorFlag, "E must lose the dictionary race in the hot index")
		}
	}
	sendStream(t, acH, model.StreamDictionary, 0, wire.DictionaryStream(sealDictWords))
	require.Eventually(t, func() bool {
		return prH.Dictionary()[sealDictCallRed] == "call.red"
	}, 5*time.Second, 10*time.Millisecond, "dictionary must decode before the seal")

	resHOld, err := storeA.Seal(ctx, keyH, bucketOld)
	require.NoError(t, err)
	require.Len(t, resHOld.Files, 2, "A1 → short_clean, E → any_error")
	uploaderA := hotstore.NewUploader(storeA, fake)
	_, err = uploaderA.Pass(ctx)
	require.NoError(t, err)

	hotSrv := httptest.NewServer(hotread.New(storeA).Handler())
	t.Cleanup(hotSrv.Close)
	disco := &scriptedDiscovery{}
	disco.set(hotSrv.URL)
	api := httptest.NewServer(query.New(query.Options{
		Config: query.Config{
			OverlapMargin:  5 * time.Minute,
			WideRangeLimit: 30 * 24 * time.Hour,
		},
		ColdStore:    fake,
		HotDiscovery: disco,
	}).Handler())
	t.Cleanup(api.Close)

	windowFrom, windowTo := baseMs-60_000, hotMs+60_000
	// The full window in (ts_ms DESC, pk ASC) order: hot-only rows, then the
	// cold-only pod, then the overlap rows present in both tiers.
	wantOrder := []string{
		fanPodHot + "@7200010", // N2 (hot only)
		fanPodHot + "@7200005", // N1 (hot only)
		fanPodCold + "@45",     // C2 (cold only)
		fanPodCold + "@40",     // C1 (cold only)
		fanPodHot + "@10",      // E  (both tiers)
		fanPodHot + "@5",       // A1 (both tiers)
	}
	rowNames := func(page callsPage) []string {
		var got []string
		for _, call := range page.Calls {
			got = append(got, fmt.Sprintf("%s@%d", call.PK.PodName, call.TsMs-baseMs))
		}
		return got
	}

	t.Run("calls merge across the cutoff: no gap, no duplicate, cold preferred", func(t *testing.T) {
		page := getCalls(t, api, url.Values{
			"from": {fmt.Sprint(windowFrom)}, "to": {fmt.Sprint(windowTo)},
		})
		assert.False(t, page.Partial)
		assert.Nil(t, page.NextCursor)
		require.Equal(t, wantOrder, rowNames(page),
			"hot and cold rows interleave in the shared order; overlap rows appear once (02 §4.3, §6)")

		e := page.Calls[4]
		assert.True(t, e.ErrorFlag, "the merged E is the cold copy: seal re-derived error_flag (02 §6.3)")
		assert.Equal(t, hotstore.RetentionAnyError, e.RetentionClass)

		n1 := page.Calls[1]
		assert.Equal(t, "com.example.Service.process", n1.Method,
			"hot methods resolve against the live dictionary")
		a1 := page.Calls[5]
		assert.Equal(t, map[string][]string{"request.id": {"req-1"}}, a1.Params)
	})

	t.Run("cutoff: a window inside every hot window skips the cold LIST", func(t *testing.T) {
		fake.reset()
		page := getCalls(t, api, url.Values{
			"from": {fmt.Sprint(hotMs - 15*60_000)}, "to": {fmt.Sprint(windowTo)},
		})
		require.Equal(t, []string{fanPodHot + "@7200010", fanPodHot + "@7200005"}, rowNames(page))
		assert.False(t, page.Partial)
		assert.Empty(t, fake.listedPrefixes(),
			"coldTo = max(hot oldest) + overlap sits below from: no S3 LIST at all (02 §4.3)")
	})

	t.Run("cutoff: a week-wide window reads both tiers", func(t *testing.T) {
		fake.reset()
		page := getCalls(t, api, url.Values{
			"from": {fmt.Sprint(baseMs - 7*24*3600*1000)}, "to": {fmt.Sprint(windowTo)},
		})
		assert.Equal(t, wantOrder, rowNames(page))
		assert.NotEmpty(t, fake.listedPrefixes(), "the cold tier is consulted for the old range")
	})

	t.Run("pods union: live hot + closed cold manifests, §2.7 shape", func(t *testing.T) {
		fake.reset()
		pods := getPods(t, api, url.Values{"from": {fmt.Sprint(windowFrom)}, "to": {fmt.Sprint(windowTo)}})
		require.Len(t, pods.Pods, 2)

		c := pods.Pods[0]
		assert.Equal(t, fanPodCold, c.Pod)
		assert.Equal(t, keyC.RestartTimeMs, c.RestartTimeMs)
		assert.Equal(t, baseMs+40, c.TimeMinMs, "cold bounds come from the pods/v1 manifest")
		assert.Equal(t, baseMs+45, c.TimeMaxMs)

		h := pods.Pods[1]
		assert.Equal(t, fanPodHot, h.Pod)
		assert.Equal(t, keyH.RestartTimeMs, h.RestartTimeMs)
		assert.Equal(t, baseMs+5, h.TimeMinMs)
		assert.Equal(t, hotMs+10, h.TimeMaxMs,
			"a pod present in both tiers is one entry with union bounds (02 §2.7)")

		for _, key := range fake.openedKeys() {
			assert.NotContains(t, key, ".parquet", "cold /pods reads manifests only")
		}
	})

	t.Run("internal API: single row, blob = seal assembler output, dictionary", func(t *testing.T) {
		pkPath := fmt.Sprintf("%s:%s:%s:%d:1:%d:0",
			hotstoreNs, hotstoreSvc, fanPodHot, keyH.RestartTimeMs, offH1[0])

		resp, err := http.Get(hotSrv.URL + "/internal/v1/calls/" + pkPath)
		require.NoError(t, err)
		_ = resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		resp, err = http.Get(hotSrv.URL + "/internal/v1/calls/" + pkPath + "/trace")
		require.NoError(t, err)
		blob, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", blob)
		assert.Equal(t, "application/octet-stream", resp.Header.Get("Content-Type"))
		assert.NotEmpty(t, resp.Header.Get("ETag"))

		var shortKey string
		for _, f := range resHOld.Files {
			if f.RetentionClass == hotstore.RetentionShortClean {
				shortKey = f.S3Key
			}
		}
		require.NotEmpty(t, shortKey)
		assert.Equal(t, sealedTraceBlob(t, fake, shortKey), blob,
			"the internal trace endpoint reuses the seal assembler byte for byte (01 §4.3/§4.5)")

		dictPath := fmt.Sprintf("%s/internal/v1/pods/%s:%s:%s:%d/dictionary",
			hotSrv.URL, hotstoreNs, hotstoreSvc, fanPodHot, keyH.RestartTimeMs)
		dictResp, err := http.Get(dictPath)
		require.NoError(t, err)
		dictBody, err := io.ReadAll(dictResp.Body)
		_ = dictResp.Body.Close()
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, dictResp.StatusCode)
		assert.Contains(t, string(dictBody), `"version":5`)
		assert.Contains(t, string(dictBody), "call.red")
	})

	t.Run("pagination holds across the hot→cold migration", func(t *testing.T) {
		window := url.Values{"from": {fmt.Sprint(windowFrom)}, "to": {fmt.Sprint(windowTo)}, "limit": {"2"}}
		page1 := getCalls(t, api, window)
		require.Equal(t, wantOrder[0:2], rowNames(page1), "page 1 is served by the hot tier")
		require.NotNil(t, page1.NextCursor)

		// The migration (02 §2.3.1): between the pages the replica's data is
		// sealed and uploaded, and the replica itself leaves the fan-out — as
		// if every row aged out of the hot tier.
		_, err := storeA.Seal(ctx, keyH, bucketNew)
		require.NoError(t, err)
		_, err = uploaderA.Pass(ctx)
		require.NoError(t, err)
		disco.set()

		page2 := getCalls(t, api, url.Values{"cursor": {*page1.NextCursor}, "limit": {"2"}})
		require.Equal(t, wantOrder[2:4], rowNames(page2),
			"page 2 continues at the exact position from the cold tier alone")
		require.NotNil(t, page2.NextCursor)

		page3 := getCalls(t, api, url.Values{"cursor": {*page2.NextCursor}, "limit": {"2"}})
		require.Equal(t, wantOrder[4:6], rowNames(page3),
			"the boundary rows appear exactly once: no duplicate, no gap")
		assert.True(t, page3.Calls[0].ErrorFlag, "E still surfaces the cold-derived error_flag")
		assert.Nil(t, page3.NextCursor, "the window is exhausted")
	})
}

// mustCalls reads one bucket's index rows of a pod-restart.
func mustCalls(t *testing.T, store *hotstore.Store, bucket int64, key hotstore.PodRestartKey) []hotstore.CallIndexRow {
	t.Helper()
	rows, err := store.Calls(bucket)
	require.NoError(t, err)
	var out []hotstore.CallIndexRow
	for _, row := range rows {
		if row.PodRestart == key.String() {
			out = append(out, row)
		}
	}
	return out
}

// sealedTraceBlob reads the trace_blob of the single row of one uploaded
// parquet object.
func sealedTraceBlob(t *testing.T, fake *coldFakeStore, key string) []byte {
	t.Helper()
	obj, err := fake.Open(context.Background(), key)
	require.NoError(t, err)
	defer func() { _ = obj.Close() }()
	rows := readParquetRows[storageparquet.CallV2](t, obj)
	require.Len(t, rows, 1)
	require.NotNil(t, rows[0].TraceBlob)
	return rows[0].TraceBlob
}
