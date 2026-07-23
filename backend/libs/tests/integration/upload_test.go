package integration

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/collector/hotstore"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	model "github.com/Netcracker/qubership-profiler-backend/libs/protocol"
	"github.com/Netcracker/qubership-profiler-backend/libs/tests/helpers/wire"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeObjectStore is the in-test S3: a key→bytes map with per-key fault
// injection, so the C1 crash window and the §8 quarantine path are drivable
// without a real endpoint. The real-client wiring is covered by the tagged
// MinIO test.
type fakeObjectStore struct {
	mu       sync.Mutex
	objects  map[string][]byte
	puts     map[string]int
	failOnce map[string]error
	reject   map[string]error
}

func newFakeObjectStore() *fakeObjectStore {
	return &fakeObjectStore{
		objects:  map[string][]byte{},
		puts:     map[string]int{},
		failOnce: map[string]error{},
		reject:   map[string]error{},
	}
}

func (f *fakeObjectStore) PutFile(_ context.Context, key, localPath string) error {
	data, err := os.ReadFile(localPath)
	if err != nil {
		return err
	}
	return f.put(key, data)
}

func (f *fakeObjectStore) PutBytes(_ context.Context, key string, body []byte) error {
	return f.put(key, append([]byte(nil), body...))
}

func (f *fakeObjectStore) put(key string, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.puts[key]++
	if err, ok := f.reject[key]; ok {
		return err
	}
	if err, ok := f.failOnce[key]; ok {
		delete(f.failOnce, key)
		return err
	}
	f.objects[key] = data
	return nil
}

// seed places an object without counting a PUT: the state a crash between a
// confirmed PUT and the metadata commit leaves behind (invariant C1).
func (f *fakeObjectStore) seed(key string, data []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[key] = append([]byte(nil), data...)
}

func (f *fakeObjectStore) object(t *testing.T, key string) []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	data, ok := f.objects[key]
	require.True(t, ok, "object %s must exist; got %v", key, f.keysLocked())
	return append([]byte(nil), data...)
}

func (f *fakeObjectStore) has(key string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.objects[key]
	return ok
}

func (f *fakeObjectStore) putCount(key string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.puts[key]
}

func (f *fakeObjectStore) keysLocked() []string {
	keys := make([]string, 0, len(f.objects))
	for k := range f.objects {
		keys = append(keys, k)
	}
	return keys
}

func (f *fakeObjectStore) allKeys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.keysLocked()
}

// TestUploadPass drives the Stage 1 S3 slice acceptance over one pod-restart
// whose calls span two UTC days (base is 2023-11-14T22:13Z; the third call
// lands past midnight):
//
//  1. happy path — sealed files PUT at their exact parquet_local.s3_key,
//     uploaded_at set, refcounts released once, refcount-0 segments unlinked
//     from disk and catalog;
//  2. C1 — one object is pre-seeded as if a crash hit between the PUT and the
//     metadata commit: the pass re-PUTs the same key (no duplicate) and
//     releases exactly once; a second pass changes nothing;
//  3. manifests — a pods manifest per sealed day with the day's
//     time_min/time_max, and NO dictionary or suspend snapshot: sealed rows
//     are self-contained (01-write-contract.md §3.6, №3/№23);
//  4. 4xx — a rejected file moves to upload-failed/ and keeps its segment
//     refcounts pinned (§8); a retryable failure is retried with backoff.
func TestUploadPass(t *testing.T) {
	ctx, cancel := context.WithCancel(log.SetLevel(context.Background(), log.INFO))
	defer cancel()
	dataDir := t.TempDir()

	svc := startCollector(t, ctx, dataDir)
	store := svc.Store()

	// Day 1: call A (clean) and call B (errored) on separate threads; day 2:
	// call C, two hours later, past UTC midnight.
	file1, off1 := wire.TraceStream(timerStartMs, []wire.TraceChunk{
		{ThreadId: sealThread1, StartMs: baseMs, Events: []wire.TraceEvent{
			wire.Enter(0, sealMethodHandle), wire.Exit(1),
		}},
		{ThreadId: sealThread2, StartMs: baseMs + 8, Events: []wire.TraceEvent{
			wire.Enter(0, sealMethodQuery), wire.Exit(2),
		}},
	})
	day2Ms := baseMs + 2*60*60*1000
	file2, off2 := wire.TraceStream(timerStartMs, []wire.TraceChunk{
		{ThreadId: sealThread3, StartMs: day2Ms, Events: []wire.TraceEvent{
			wire.Enter(0, sealMethodHandle), wire.Exit(1),
		}},
	})

	calls := []wire.CallRecord{
		{DeltaMs: 5, Method: sealMethodHandle, DurationMs: 10, ThreadName: "exec-1", // A, ts base+5
			TraceFileIndex: 1, BufferOffset: int(off1[0]), RecordIndex: 0},
		{DeltaMs: 5, Method: sealMethodQuery, DurationMs: 20, ThreadName: "exec-2", // B, ts base+10, errored
			TraceFileIndex: 1, BufferOffset: int(off1[1]), RecordIndex: 0,
			Params: map[int][]string{sealDictCallRed: {"1"}}},
		{DeltaMs: 2*60*60*1000 - 10, Method: sealMethodHandle, DurationMs: 10, ThreadName: "exec-3", // C, ts base+2h
			TraceFileIndex: 2, BufferOffset: int(off2[0]), RecordIndex: 0},
	}

	ac := connectAgent(t, ctx)
	key := waitForPodRestart(t, store)
	pr, ok := store.PodRestart(key)
	require.True(t, ok)

	sendStream(t, ac, model.StreamDictionary, 0, wire.DictionaryStream(sealDictWords))
	sendStream(t, ac, model.StreamSuspend, 0, wire.SuspendStream(baseMs, []wire.SuspendEvent{
		{DeltaMs: 50, AmountMs: 30}, // pause ending at base+50 (№4)
		{DeltaMs: 55, AmountMs: 20}, // pause ending at base+105
	}))
	sendStream(t, ac, model.StreamTrace, 0, file1)
	sendStream(t, ac, model.StreamTrace, 1, file2)
	sendStream(t, ac, model.StreamCalls, 0, wire.CallsStreamRecords(baseMs, calls))

	require.NoError(t, ac.Flush())
	require.NoError(t, ac.WaitForAcks())
	require.NoError(t, ac.CommandClose())
	_ = ac.Close()
	require.Eventually(t, pr.Finalized, 5*time.Second, 10*time.Millisecond,
		"disconnect must finalize the pod-restart")

	bucket1 := store.Config().Bucket(baseMs + 5)
	bucket2 := store.Config().Bucket(day2Ms)
	res1, err := store.Seal(ctx, key, bucket1)
	require.NoError(t, err)
	require.Len(t, res1.Files, 2, "day 1 seals short_clean (A) and any_error (B)")
	res2, err := store.Seal(ctx, key, bucket2)
	require.NoError(t, err)
	require.Len(t, res2.Files, 1, "day 2 seals short_clean (C)")

	byClass := map[string]hotstore.SealedFile{}
	for _, f := range res1.Files {
		byClass[f.RetentionClass] = f
	}
	fileA := byClass[hotstore.RetentionShortClean]
	fileB := byClass[hotstore.RetentionAnyError]
	fileC := res2.Files[0]
	localBytes := func(path string) []byte {
		data, err := os.ReadFile(path)
		require.NoError(t, err)
		return data
	}

	requireRefcounts(t, store, key, map[int]int{1: 2, 2: 1})

	fake := newFakeObjectStore()
	// Retryable failure on A: the pass must retry with backoff and succeed.
	fake.failOnce[fileA.S3Key] = errors.New("503 slow down")
	// Permanent rejection on B: the §8 quarantine path.
	fake.reject[fileB.S3Key] = &hotstore.PermanentUploadError{Err: errors.New("403 AccessDenied")}
	// C1 crash window on C: its PUT succeeded but the metadata commit did not.
	fake.seed(fileC.S3Key, []byte("stale bytes of the pre-crash PUT"))

	uploader := hotstore.NewUploader(store, fake)
	stats, err := uploader.Pass(ctx)
	require.NoError(t, err)

	hash := hotstore.PodRestartHash(key)
	manifestDay1Key := fmt.Sprintf("pods/v1/2023/11/14/%s.json", hash)
	manifestDay2Key := fmt.Sprintf("pods/v1/2023/11/15/%s.json", hash)

	t.Run("happy path: objects at exact keys, uploaded_at, single release", func(t *testing.T) {
		assert.Equal(t, localBytes(fileA.Path), fake.object(t, fileA.S3Key),
			"A must be PUT byte-identical at its recorded s3_key")
		assert.Equal(t, localBytes(fileC.Path), fake.object(t, fileC.S3Key),
			"C must overwrite the pre-crash object at the same deterministic key")
		assert.Equal(t, 1, fake.putCount(fileC.S3Key), "one idempotent re-PUT, no duplicate key")
		assert.Equal(t, 2, fake.putCount(fileA.S3Key), "one failed attempt plus the successful retry")

		uploadedAt := map[string]*int64{}
		failedAt := map[string]*int64{}
		files, err := store.LocalParquet(key)
		require.NoError(t, err)
		require.Len(t, files, 3)
		for _, f := range files {
			uploadedAt[f.RetentionClass+f.S3Key] = f.UploadedAtMs
			failedAt[f.RetentionClass+f.S3Key] = f.UploadFailedAtMs
		}
		assert.NotNil(t, uploadedAt[hotstore.RetentionShortClean+fileA.S3Key])
		assert.NotNil(t, uploadedAt[hotstore.RetentionShortClean+fileC.S3Key])
		assert.Nil(t, failedAt[hotstore.RetentionShortClean+fileA.S3Key])

		assert.FileExists(t, fileA.Path, "the local copy backs the hot tier past upload (§6.3)")
		assert.FileExists(t, fileC.Path)

		assert.EqualValues(t, 2, stats.UploadedFiles)
		assert.GreaterOrEqual(t, stats.RetriedPuts, int64(1))
	})

	t.Run("C1 + refcounts: released exactly once, pinned segment survives", func(t *testing.T) {
		// B is quarantined, so its 1 row keeps segment 1 pinned; segment 2 (C's
		// only source) reached zero via the release and is gone — disk and
		// catalog together. A double release would have freed segment 1 too.
		requireRefcounts(t, store, key, map[int]int{1: 1})
		podDir := filepath.Join(dataDir, "pods", hotstoreNs, hotstoreSvc, hotstorePod,
			fmt.Sprintf("%d", key.RestartTimeMs))
		assert.FileExists(t, filepath.Join(podDir, "trace", "000001.gz"))
		assert.NoFileExists(t, filepath.Join(podDir, "trace", "000002.gz"))
		assert.EqualValues(t, 1, stats.SegmentsDeleted)
	})

	t.Run("4xx: quarantined under upload-failed/, refcounts kept", func(t *testing.T) {
		assert.False(t, fake.has(fileB.S3Key), "a rejected object must not exist in S3")
		assert.NoFileExists(t, fileB.Path, "the rejected file leaves its sealed path")
		// The quarantine mirrors the full S3 key, not just the basename: the sealed
		// basename omits the retention class, so two classes can share one and would
		// collide in a flat directory (QA 708#3).
		quarantined := filepath.Join(dataDir, "upload-failed", filepath.FromSlash(fileB.S3Key))
		assert.Equal(t, localBytes(quarantined), func() []byte {
			data, err := os.ReadFile(quarantined)
			require.NoError(t, err)
			return data
		}(), "the parquet must survive intact under upload-failed/")

		files, err := store.LocalParquet(key)
		require.NoError(t, err)
		for _, f := range files {
			if f.RetentionClass != hotstore.RetentionAnyError {
				continue
			}
			assert.Nil(t, f.UploadedAtMs, "a rejected file is not uploaded")
			require.NotNil(t, f.UploadFailedAtMs, "the quarantine must be recorded")
			assert.Equal(t, quarantined, f.Path, "the catalog row follows the file")
		}
		assert.EqualValues(t, 1, stats.QuarantinedFiles)
	})

	t.Run("per-day pods manifests, and no snapshot objects", func(t *testing.T) {
		manifest := func(timeMinMs, timeMaxMs int64) string {
			return fmt.Sprintf(
				`{"namespace":%q,"service":%q,"pod":%q,"restart_time_ms":%d,"timer_start_ms":%d,"replica":"collector-0","time_min_ms":%d,"time_max_ms":%d}`,
				hotstoreNs, hotstoreSvc, hotstorePod, key.RestartTimeMs, timerStartMs, timeMinMs, timeMaxMs)
		}
		assert.JSONEq(t, manifest(baseMs+5, baseMs+10), string(fake.object(t, manifestDay1Key)),
			"day-1 bounds span A and B — the quarantined file still names sealed data")
		assert.JSONEq(t, manifest(day2Ms, day2Ms), string(fake.object(t, manifestDay2Key)))
		assert.EqualValues(t, 2, stats.ManifestPuts)

		// №3/№23: sealed rows are self-contained, so the uploader writes no
		// dictionary or suspend snapshot at all.
		for _, k := range fake.allKeys() {
			assert.False(t, strings.HasPrefix(k, "dictionaries/") || strings.HasPrefix(k, "suspend/"),
				"no snapshot object may be uploaded: %s", k)
		}
	})

	t.Run("a second pass is a full no-op", func(t *testing.T) {
		before := map[string]int{}
		for _, k := range []string{fileA.S3Key, fileB.S3Key, fileC.S3Key,
			manifestDay1Key, manifestDay2Key} {
			before[k] = fake.putCount(k)
		}
		again, err := uploader.Pass(ctx)
		require.NoError(t, err)
		assert.Equal(t, hotstore.UploadStats{}, again,
			"nothing is pending: no re-upload, no quarantine retry (the upload_failed_at gate)")
		for k, n := range before {
			assert.Equal(t, n, fake.putCount(k), "no further PUT of %s", k)
		}
		requireRefcounts(t, store, key, map[int]int{1: 1})
	})
}

// TestUploadRecovery pins the restart half of the slice (03-lifecycle.md
// §3.8-§3.9 and invariant C1): a collector that crashed after sealing — with
// the parquet PUT already confirmed but the metadata commit lost — recovers,
// re-uploads the pending file idempotently, releases the refcounts exactly
// once, and refreshes the pods manifest with the trace epoch re-read from
// the segment.
func TestUploadRecovery(t *testing.T) {
	ctx, cancel := context.WithCancel(log.SetLevel(context.Background(), log.INFO))
	defer cancel()
	dataDir := t.TempDir()

	svc := startCollector(t, ctx, dataDir)
	store := svc.Store()

	file1, off1 := wire.TraceStream(timerStartMs, []wire.TraceChunk{
		{ThreadId: sealThread1, StartMs: baseMs, Events: []wire.TraceEvent{
			wire.Enter(0, sealMethodHandle), wire.Exit(1),
		}},
	})
	calls := []wire.CallRecord{
		{DeltaMs: 5, Method: sealMethodHandle, DurationMs: 10, ThreadName: "exec-1",
			TraceFileIndex: 1, BufferOffset: int(off1[0]), RecordIndex: 0},
	}

	ac := connectAgent(t, ctx)
	key := waitForPodRestart(t, store)
	pr, ok := store.PodRestart(key)
	require.True(t, ok)
	sendStream(t, ac, model.StreamDictionary, 0, wire.DictionaryStream(sealDictWords))
	sendStream(t, ac, model.StreamTrace, 0, file1)
	sendStream(t, ac, model.StreamCalls, 0, wire.CallsStreamRecords(baseMs, calls))
	require.NoError(t, ac.Flush())
	require.NoError(t, ac.WaitForAcks())
	require.NoError(t, ac.CommandClose())
	_ = ac.Close()
	require.Eventually(t, pr.Finalized, 5*time.Second, 10*time.Millisecond)

	res, err := store.Seal(ctx, key, store.Config().Bucket(baseMs+5))
	require.NoError(t, err)
	require.Len(t, res.Files, 1)
	sealed := res.Files[0]
	sealedData, err := os.ReadFile(sealed.Path)
	require.NoError(t, err)

	// Crash: the PUT reached S3 but neither uploaded_at nor the refcount
	// release committed, and no snapshot went out.
	fake := newFakeObjectStore()
	fake.seed(sealed.S3Key, []byte("bytes of the PUT the crash orphaned"))
	cancel()
	waitForCollectorStop(t, svc)

	store2, err := hotstore.Open(hotstore.Config{DataDir: dataDir})
	require.NoError(t, err)
	defer func() { _ = store2.Close() }()
	require.NoError(t, store2.Recover(context.Background()))
	requireRefcounts(t, store2, key, map[int]int{1: 1})

	stats, err := hotstore.NewUploader(store2, fake).Pass(context.Background())
	require.NoError(t, err)
	assert.EqualValues(t, 1, stats.UploadedFiles)
	assert.EqualValues(t, 1, stats.ManifestPuts)
	assert.EqualValues(t, 1, stats.SegmentsDeleted)

	assert.Equal(t, sealedData, fake.object(t, sealed.S3Key),
		"recovery re-PUTs the same deterministic key over the orphaned object")
	assert.Equal(t, 1, fake.putCount(sealed.S3Key), "exactly one re-PUT, no duplicate object")
	requireRefcounts(t, store2, key, map[int]int{},
		"the single release brought the only segment to zero and the sweep unlinked it")

	files, err := store2.LocalParquet(key)
	require.NoError(t, err)
	require.Len(t, files, 1)
	assert.NotNil(t, files[0].UploadedAtMs)

	hash := hotstore.PodRestartHash(key)
	day := time.UnixMilli(baseMs).UTC().Format("2006/01/02")
	manifestKey := fmt.Sprintf("pods/v1/%s/%s.json", day, hash)
	assert.JSONEq(t, fmt.Sprintf(
		`{"namespace":%q,"service":%q,"pod":%q,"restart_time_ms":%d,"timer_start_ms":%d,"replica":"collector-0","time_min_ms":%d,"time_max_ms":%d}`,
		hotstoreNs, hotstoreSvc, hotstorePod, key.RestartTimeMs, timerStartMs, baseMs+5, baseMs+5),
		string(fake.object(t, manifestKey)),
		"timer_start_ms must be re-read from the trace segment after the restart")
}

// requireRefcounts asserts the trace-segment refcounts by rolling_seq; a
// segment absent from want must be absent from the catalog too.
func requireRefcounts(t *testing.T, store *hotstore.Store, key hotstore.PodRestartKey, want map[int]int, msgAndArgs ...any) {
	t.Helper()
	segments, err := store.Segments(key)
	require.NoError(t, err)
	got := map[int]int{}
	for _, seg := range segments {
		if !strings.EqualFold(seg.Stream, hotstore.StreamTrace) {
			continue
		}
		got[seg.RollingSeq] = seg.Refcount
	}
	require.Equal(t, want, got, msgAndArgs...)
}
