package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/collector/hotread"
	"github.com/Netcracker/qubership-profiler-backend/libs/collector/hotstore"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	model "github.com/Netcracker/qubership-profiler-backend/libs/protocol"
	"github.com/Netcracker/qubership-profiler-backend/libs/query"
	qmodel "github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	"github.com/Netcracker/qubership-profiler-backend/libs/tests/helpers/wire"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const parityPod = "pod-parity"

// TestHotColdCallJSONParity proves the R1 projection end to end
// (08-ui-backend-requirements.md R1, 02-read-contract.md §2.3): the same
// synthetic calls render the identical CallJSON from the hot tier (the SQLite
// call index over /internal/v1/calls) and from the cold tier (sealed parquet
// over /api/v1/calls) — including the metric columns the old wire dropped:
// queue_wait_ms, suspend_ms, transactions, logs_*, file_*, net_*.
//
// The suspend timeline lands before the calls, so the index-time suspend_ms
// attribution sees the same pauses as the seal-time derivation (01 §5.1
// step 4); out of that order the hot value is provisional and only the cold
// copy is exact (02 §6.3 prefers cold on dedup for the same reason).
func TestHotColdCallJSONParity(t *testing.T) {
	ctx, cancel := context.WithCancel(log.SetLevel(context.Background(), log.INFO))
	defer cancel()

	fake := newColdFakeStore()
	svc := startCollector(t, ctx, t.TempDir())
	store := svc.Store()
	bucket := store.Config().Bucket(baseMs + 5)

	traceFile, offs := wire.TraceStream(timerStartMs, []wire.TraceChunk{
		{ThreadId: 71, StartMs: baseMs + 5, Events: []wire.TraceEvent{
			wire.Enter(0, sealMethodHandle), wire.Tag(1, sealDictRequestId, "req-9"), wire.Exit(2),
		}},
		{ThreadId: 72, StartMs: baseMs + 30, Events: []wire.TraceEvent{
			wire.Enter(0, sealMethodQuery), wire.Exit(1),
		}},
	})
	calls := []wire.CallRecord{
		// H carries every version-4 metric; its [5, 55) interval overlaps the
		// [10, 30) pause by 20 ms.
		{DeltaMs: 5, Method: sealMethodHandle, DurationMs: 50, ChildCalls: 3, ThreadName: "exec-1",
			TraceFileIndex: 1, BufferOffset: int(offs[0]), RecordIndex: 0,
			Params:    map[int][]string{sealDictRequestId: {"req-9"}},
			CpuTimeMs: 37, WaitTimeMs: 4, MemoryUsed: 1 << 20,
			LogsGenerated: 2048, LogsWritten: 512,
			FileRead: 100, FileWritten: 200, NetRead: 300, NetWritten: 400,
			Transactions: 2, QueueWaitMs: 15},
		// Q starts at +30, right after the pause ends: zero suspend overlap,
		// long_clean class, all counters zero.
		{DeltaMs: 25, Method: sealMethodQuery, DurationMs: 1500, ThreadName: "exec-2",
			TraceFileIndex: 1, BufferOffset: int(offs[1]), RecordIndex: 0},
	}

	ac := connectAgentAs(t, ctx, parityPod)
	key := waitForPodNamed(t, store, parityPod, 1)
	pr, ok := store.PodRestart(key)
	require.True(t, ok)
	sendStream(t, ac, model.StreamDictionary, 0, wire.DictionaryStream(sealDictWords))
	sendStream(t, ac, model.StreamSuspend, 0, wire.SuspendStream(baseMs, []wire.SuspendEvent{
		{DeltaMs: 10, AmountMs: 20},
	}))
	// Barrier: the suspend stream decodes on its own pipeline; the pause must
	// be in RAM before the calls below are indexed for the tiers to agree.
	require.Eventually(t, func() bool { return len(pr.SuspendPauses()) == 1 },
		5*time.Second, 10*time.Millisecond, "the pause must land before the calls are indexed")

	sendStream(t, ac, model.StreamTrace, 0, traceFile)
	sendStream(t, ac, model.StreamCalls, 0, wire.CallsStreamRecords(baseMs, calls))
	waitForIndexedCalls(t, store, bucket, key, 2)

	hotSrv := httptest.NewServer(hotread.New(store).Handler())
	t.Cleanup(hotSrv.Close)
	window := url.Values{"from": {fmt.Sprint(baseMs - 60_000)}, "to": {fmt.Sprint(baseMs + 60_000)}}
	hotCalls := decodeCallsJSON(t, hotSrv.URL+"/internal/v1/calls?"+window.Encode())

	// Move the same data to the cold tier: close, seal, upload, and read it
	// back through a query service with no hot replicas at all.
	require.NoError(t, ac.CommandClose())
	_ = ac.Close()
	require.Eventually(t, pr.Finalized, 5*time.Second, 10*time.Millisecond)
	_, err := store.Seal(ctx, key, bucket)
	require.NoError(t, err)
	_, err = hotstore.NewUploader(store, fake).Pass(ctx)
	require.NoError(t, err)

	api := httptest.NewServer(query.New(query.Options{
		Config: query.Config{
			OverlapMargin:  5 * time.Minute,
			WideRangeLimit: 30 * 24 * time.Hour,
		},
		ColdStore:    fake,
		HotDiscovery: &scriptedDiscovery{},
	}).Handler())
	t.Cleanup(api.Close)
	coldCalls := decodeCallsJSON(t, api.URL+"/api/v1/calls?"+window.Encode())

	require.Len(t, hotCalls, 2)
	require.Equal(t, hotCalls, coldCalls,
		"both tiers must render the identical CallJSON (02 §2.3, 08 R1)")

	// The projected columns carry the synthetic values, not zeros. Rows come
	// back (ts_ms DESC, pk ASC): Q at +30 first, H at +5 second.
	h, q := hotCalls[1], hotCalls[0]
	assert.Equal(t, "com.example.Service.handle", h.Method)
	assert.Equal(t, int64(37), h.CpuTimeMs)
	assert.Equal(t, int64(4), h.WaitTimeMs)
	assert.Equal(t, int64(1<<20), h.MemoryUsed)
	assert.Equal(t, int32(15), h.QueueWaitMs)
	assert.Equal(t, int32(20), h.SuspendMs, "call [5, 55) ∩ pause [10, 30) = 20 ms")
	assert.Equal(t, int32(3), h.ChildCalls)
	assert.Equal(t, int32(2), h.Transactions)
	assert.Equal(t, int64(2048), h.LogsGenerated)
	assert.Equal(t, int64(512), h.LogsWritten)
	assert.Equal(t, int64(100), h.FileRead)
	assert.Equal(t, int64(200), h.FileWritten)
	assert.Equal(t, int64(300), h.NetRead)
	assert.Equal(t, int64(400), h.NetWritten)

	assert.Equal(t, "com.example.Db.query", q.Method)
	assert.Equal(t, int32(0), q.SuspendMs, "no pause overlaps [30, 1530)")
	assert.Equal(t, qmodel.RetentionLongClean, q.RetentionClass)
}

// decodeCallsJSON decodes one /calls page into the shared wire struct, so the
// parity check covers the full CallJSON shape rather than a hand-picked
// subset.
func decodeCallsJSON(t *testing.T, callsUrl string) []qmodel.CallJSON {
	t.Helper()
	resp, err := http.Get(callsUrl)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "unexpected status, body: %s", body)
	var page struct {
		Calls []qmodel.CallJSON `json:"calls"`
	}
	require.NoError(t, json.Unmarshal(body, &page))
	return page.Calls
}
