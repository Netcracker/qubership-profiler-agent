package integration

import (
	"context"
	"fmt"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
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

const janitorMinuteMs = int64(60_000)

// TestJanitorHotDropZeroGap drives the Stage 1 lifecycle-track acceptance 1
// and 3: a bucket leaves the hot tier only past upload + hot_retention (the
// SQLite partition, the local parquet, and the refcount-0 segments all go),
// and the same rows keep answering /api/v1/calls from S3 — no gap across the
// drop (01 §6.3, 02 §4.2-§4.3). The WALs go later, after the purge grace, and
// recovery over the purged PV comes up clean while cold still serves.
func TestJanitorHotDropZeroGap(t *testing.T) {
	ctx, cancel := context.WithCancel(log.SetLevel(context.Background(), log.INFO))
	defer cancel()
	fake := newColdFakeStore()
	dataDir := t.TempDir()

	ctxA, cancelA := context.WithCancel(ctx)
	svc := startCollector(t, ctxA, dataDir)
	store := svc.Store()
	bucket := store.Config().Bucket(baseMs + 5)

	file1, off1 := wire.TraceStream(timerStartMs, []wire.TraceChunk{
		{ThreadId: sealThread1, StartMs: baseMs, Events: []wire.TraceEvent{
			wire.Enter(0, sealMethodHandle), wire.Tag(1, sealDictRequestId, "req-j"), wire.Exit(2),
		}},
		{ThreadId: sealThread2, StartMs: baseMs + 8, Events: []wire.TraceEvent{
			wire.Enter(0, sealMethodQuery), wire.Exit(1),
		}},
	})
	calls := []wire.CallRecord{
		{DeltaMs: 5, Method: sealMethodHandle, DurationMs: 10, ThreadName: "exec-1",
			TraceFileIndex: 1, BufferOffset: int(off1[0]), RecordIndex: 0,
			Params: map[int][]string{sealDictRequestId: {"req-j"}}},
		{DeltaMs: 5, Method: sealMethodQuery, DurationMs: 20, ThreadName: "exec-2",
			TraceFileIndex: 1, BufferOffset: int(off1[1]), RecordIndex: 0},
	}

	ac := connectAgentAs(t, ctx, "pod-janitor")
	key := waitForPodNamed(t, store, "pod-janitor", 1)
	pr, ok := store.PodRestart(key)
	require.True(t, ok)
	sendStream(t, ac, model.StreamDictionary, 0, wire.DictionaryStream(sealDictWords))
	require.Eventually(t, func() bool {
		return pr.Dictionary()[sealDictCallRed] == "call.red"
	}, 5*time.Second, 10*time.Millisecond)
	sendStream(t, ac, model.StreamTrace, 0, file1)
	sendStream(t, ac, model.StreamCalls, 0, wire.CallsStreamRecords(baseMs, calls))
	waitForIndexedCalls(t, store, bucket, key, 2)
	require.NoError(t, ac.CommandClose())
	_ = ac.Close()
	require.Eventually(t, pr.Finalized, 5*time.Second, 10*time.Millisecond)

	res, err := store.Seal(ctx, key, bucket)
	require.NoError(t, err)
	require.Len(t, res.Files, 1, "both calls are short_clean")
	localParquet := res.Files[0].Path
	_, err = hotstore.NewUploader(store, fake).Pass(ctx)
	require.NoError(t, err)
	now := time.Now().UnixMilli()

	hotSrv := httptest.NewServer(hotread.New(store).Handler())
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

	window := url.Values{"from": {fmt.Sprint(baseMs - janitorMinuteMs)}, "to": {fmt.Sprint(baseMs + janitorMinuteMs)}}
	rowIds := func(page callsPage) []string {
		var got []string
		for _, call := range page.Calls {
			got = append(got, fmt.Sprintf("%s@%d:%s:%v", call.PK.PodName, call.TsMs-baseMs,
				call.Method, call.Params))
		}
		return got
	}
	before := getCalls(t, api, window)
	require.Len(t, before.Calls, 2, "both tiers hold the rows before the janitor")

	podDir := filepath.Join(dataDir, "pods", hotstoreNs, hotstoreSvc, "pod-janitor",
		fmt.Sprint(key.RestartTimeMs))
	dictWal := filepath.Join(podDir, "dictionary.wal")
	require.FileExists(t, dictWal)

	t.Run("inside hot retention nothing is dropped", func(t *testing.T) {
		stats, err := store.JanitorPass(ctx, now)
		require.NoError(t, err)
		assert.Zero(t, stats.ParquetDeleted)
		assert.Zero(t, stats.PartitionsDropped)
		_, ok, err := store.HotWindowOldestMs()
		require.NoError(t, err)
		assert.True(t, ok, "the hot index still holds the bucket")
	})

	t.Run("past hot retention the bucket leaves the hot tier", func(t *testing.T) {
		stats, err := store.JanitorPass(ctx, now+16*janitorMinuteMs)
		require.NoError(t, err)
		assert.EqualValues(t, 1, stats.ParquetDeleted)
		assert.EqualValues(t, 1, stats.PartitionsDropped)

		buckets, err := store.Buckets()
		require.NoError(t, err)
		assert.Empty(t, buckets, "the call-index partition is gone")
		partitions, err := filepath.Glob(filepath.Join(dataDir, "calls-*.sqlite"))
		require.NoError(t, err)
		assert.Empty(t, partitions, "the partition SQLite file is gone")
		assert.NoFileExists(t, localParquet, "the local sealed parquet is gone")
		segments, err := filepath.Glob(filepath.Join(podDir, "trace", "*.gz"))
		require.NoError(t, err)
		assert.Empty(t, segments, "refcount-0 segments are gone after upload")
		assert.FileExists(t, dictWal, "WALs wait for the purge grace")

		_, ok, err := store.HotWindowOldestMs()
		require.NoError(t, err)
		assert.False(t, ok, "the hot window is empty after the drop")
	})

	t.Run("the dropped rows keep answering from S3 — zero gap", func(t *testing.T) {
		fake.reset()
		after := getCalls(t, api, window)
		assert.Equal(t, rowIds(before), rowIds(after),
			"the same rows, same order, across the hot→cold drop")
		assert.False(t, after.Partial)
		assert.NotEmpty(t, fake.listedPrefixes(), "the answer can only come from the cold tier now")
	})

	t.Run("past the grace the WALs are purged", func(t *testing.T) {
		stats, err := store.JanitorPass(ctx, now+2*60*janitorMinuteMs)
		require.NoError(t, err)
		assert.EqualValues(t, 1, stats.WalsPurged)
		assert.NoFileExists(t, dictWal)
		assert.NoDirExists(t, podDir, "the fully flushed pod-restart leaves the PV")
	})

	t.Run("recovery after the purge comes up clean and cold still serves", func(t *testing.T) {
		cancelA()
		waitForCollectorStop(t, svc)
		hotSrv.Close()
		disco.set() // the replica is gone; cold is the only source

		store2, err := hotstore.Open(hotstore.Config{DataDir: dataDir})
		require.NoError(t, err)
		defer func() { _ = store2.Close() }()
		require.NoError(t, store2.Recover(ctx))
		assert.Empty(t, store2.PodRestartKeys(), "nothing to resurrect from the purged PV")
		buckets, err := store2.Buckets()
		require.NoError(t, err)
		assert.Empty(t, buckets)

		after := getCalls(t, api, window)
		assert.Equal(t, rowIds(before), rowIds(after), "S3 alone still answers after recovery")
	})
}

// TestJanitorDiskBudgetEviction drives the lifecycle-track acceptance 2: over
// PROFILER_CHUNKS_STAGING_MAX_BYTES the oldest segments are evicted first, and
// a call whose segment was evicted before its seal lands as trace_blob = NULL
// with truncated_reason = disk_budget, while a call on a surviving segment
// keeps its blob (01 §4.6). The janitor only creates the condition; the seal
// pass records it.
func TestJanitorDiskBudgetEviction(t *testing.T) {
	ctx, cancel := context.WithCancel(log.SetLevel(context.Background(), log.INFO))
	defer cancel()
	dataDir := t.TempDir()

	ctxA, cancelA := context.WithCancel(ctx)
	svc := startCollector(t, ctxA, dataDir)
	store := svc.Store()
	bucket := store.Config().Bucket(baseMs + 5)

	// Two agent stream files → two segments; one call in each.
	file1, off1 := wire.TraceStream(timerStartMs, []wire.TraceChunk{
		{ThreadId: sealThread1, StartMs: baseMs, Events: []wire.TraceEvent{
			wire.Enter(0, sealMethodHandle), wire.Exit(1),
		}},
	})
	file2, off2 := wire.TraceStream(timerStartMs, []wire.TraceChunk{
		{ThreadId: sealThread2, StartMs: baseMs + 8, Events: []wire.TraceEvent{
			wire.Enter(0, sealMethodQuery), wire.Exit(1),
		}},
	})
	calls := []wire.CallRecord{
		{DeltaMs: 5, Method: sealMethodHandle, DurationMs: 10, ThreadName: "exec-1", // X, in segment 1
			TraceFileIndex: 1, BufferOffset: int(off1[0]), RecordIndex: 0},
		{DeltaMs: 5, Method: sealMethodQuery, DurationMs: 20, ThreadName: "exec-2", // Y, in segment 2
			TraceFileIndex: 2, BufferOffset: int(off2[0]), RecordIndex: 0},
	}

	ac := connectAgentAs(t, ctx, "pod-budget")
	key := waitForPodNamed(t, store, "pod-budget", 1)
	pr, ok := store.PodRestart(key)
	require.True(t, ok)
	sendStream(t, ac, model.StreamDictionary, 0, wire.DictionaryStream(sealDictWords))
	sendStream(t, ac, model.StreamTrace, 0, file1)
	sendStream(t, ac, model.StreamTrace, 1, file2)
	sendStream(t, ac, model.StreamCalls, 0, wire.CallsStreamRecords(baseMs, calls))
	waitForIndexedCalls(t, store, bucket, key, 2)
	require.NoError(t, ac.CommandClose())
	_ = ac.Close()
	require.Eventually(t, pr.Finalized, 5*time.Second, 10*time.Millisecond)

	// Restart the store with a budget that fits exactly one of the two
	// segments: the budget is fixed at Open, and the segment sizes are only
	// known once written.
	cancelA()
	waitForCollectorStop(t, svc)
	podDir := filepath.Join(dataDir, "pods", hotstoreNs, hotstoreSvc, "pod-budget",
		fmt.Sprint(key.RestartTimeMs))
	seg1 := filepath.Join(podDir, "trace", "000001.gz")
	seg2 := filepath.Join(podDir, "trace", "000002.gz")
	info2, err := os.Stat(seg2)
	require.NoError(t, err)

	store2, err := hotstore.Open(hotstore.Config{DataDir: dataDir, ChunksStagingMaxBytes: info2.Size()})
	require.NoError(t, err)
	defer func() { _ = store2.Close() }()
	require.NoError(t, store2.Recover(ctx))

	stats, err := store2.JanitorPass(ctx, time.Now().UnixMilli())
	require.NoError(t, err)
	assert.EqualValues(t, 1, stats.SegmentsEvicted, "evicting the oldest segment satisfies the budget")
	assert.NoFileExists(t, seg1, "the oldest segment is the one evicted")
	assert.FileExists(t, seg2)

	res, err := store2.Seal(ctx, key, bucket)
	require.NoError(t, err)
	require.Len(t, res.Files, 1, "both calls are short_clean")
	assert.Equal(t, map[string]int{hotstore.TruncDiskBudget: 1}, res.Truncated)

	fake := newColdFakeStore()
	_, err = hotstore.NewUploader(store2, fake).Pass(ctx)
	require.NoError(t, err)
	obj, err := fake.Open(ctx, res.Files[0].S3Key)
	require.NoError(t, err)
	defer func() { _ = obj.Close() }()
	rows := readParquetRows[storageparquet.CallV2](t, obj)
	require.Len(t, rows, 2)
	byOffset := map[int32]storageparquet.CallV2{}
	for _, row := range rows {
		byOffset[row.TraceFileIndex] = row
	}
	x, y := byOffset[1], byOffset[2]
	require.NotNil(t, x.TruncatedReason, "X lost its segment before the seal")
	assert.Equal(t, hotstore.TruncDiskBudget, *x.TruncatedReason)
	assert.Nil(t, x.TraceBlob)
	assert.Nil(t, y.TruncatedReason, "Y's segment survived the eviction")
	assert.NotEmpty(t, y.TraceBlob)
}

// TestManifestQuarantine drives the lifecycle-track acceptance 4, mirroring
// the parquet quarantine of the S3 slice: a permanent 4xx on a pods manifest
// moves the body to upload-failed/ and stops the retry, while the parquet
// object — already durable — still commits. (The dictionary and suspend
// snapshots are gone: sealed rows are self-contained, №3/№23, so the pods
// manifest is the only snapshot object left to quarantine.)
func TestManifestQuarantine(t *testing.T) {
	ctx, cancel := context.WithCancel(log.SetLevel(context.Background(), log.INFO))
	defer cancel()
	dataDir := t.TempDir()

	svc := startCollector(t, ctx, dataDir)
	store := svc.Store()
	bucket := store.Config().Bucket(baseMs + 5)

	file, off := wire.TraceStream(timerStartMs, []wire.TraceChunk{
		{ThreadId: sealThread1, StartMs: baseMs, Events: []wire.TraceEvent{
			wire.Enter(0, sealMethodHandle), wire.Exit(1),
		}},
	})
	calls := []wire.CallRecord{
		{DeltaMs: 5, Method: sealMethodHandle, DurationMs: 10, ThreadName: "exec-1",
			TraceFileIndex: 1, BufferOffset: int(off[0]), RecordIndex: 0},
	}
	ac := connectAgentAs(t, ctx, "pod-q-manifest")
	key := waitForPodNamed(t, store, "pod-q-manifest", 1)
	pr, ok := store.PodRestart(key)
	require.True(t, ok)
	sendStream(t, ac, model.StreamDictionary, 0, wire.DictionaryStream(sealDictWords))
	sendStream(t, ac, model.StreamTrace, 0, file)
	sendStream(t, ac, model.StreamCalls, 0, wire.CallsStreamRecords(baseMs, calls))
	waitForIndexedCalls(t, store, bucket, key, 1)
	require.NoError(t, ac.CommandClose())
	_ = ac.Close()
	require.Eventually(t, pr.Finalized, 5*time.Second, 10*time.Millisecond)
	_, err := store.Seal(ctx, key, bucket)
	require.NoError(t, err)

	utcDay := func(ms int64) string { return time.UnixMilli(ms).UTC().Format("2006/01/02") }
	manifestKey := fmt.Sprintf("pods/v1/%s/%s.json", utcDay(baseMs), hotstore.PodRestartHash(key))

	fake := newFakeObjectStore()
	fake.reject[manifestKey] = &hotstore.PermanentUploadError{Err: fmt.Errorf("403 AccessDenied")}

	uploader := hotstore.NewUploader(store, fake)
	stats, err := uploader.Pass(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 1, stats.QuarantinedObjects)

	assert.False(t, fake.has(manifestKey))
	assert.FileExists(t, filepath.Join(dataDir, "upload-failed", filepath.FromSlash(manifestKey)),
		"the manifest body waits for a human under upload-failed/")
	files, err := store.LocalParquet(key)
	require.NoError(t, err)
	require.Len(t, files, 1)
	assert.NotNil(t, files[0].UploadedAtMs,
		"the parquet object is durable; a rejected manifest must not re-PUT it forever")

	puts := fake.putCount(manifestKey)
	_, err = uploader.Pass(ctx)
	require.NoError(t, err)
	assert.Equal(t, puts, fake.putCount(manifestKey), "the quarantine marker stops manifest PUTs")
}
