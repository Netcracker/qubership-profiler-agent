package hotstore

// Focus C durability tests: the №6 atomic watermark, the №7 big-value
// survival under the disk budget, the №8 poison-pill skip and recovery
// cleanup, the №9 watermark-bounded WAL reads, and the int32 clamp. Crash
// tests never close pod-restarts gracefully — Store.Close leaves the WALs
// footer-less exactly as kill -9 would, and the calls.wal tears are genuine
// truncations.

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/protocol/data"
	storageparquet "github.com/Netcracker/qubership-profiler-backend/libs/storage/parquet"
	"github.com/Netcracker/qubership-profiler-backend/libs/tests/helpers/wire"
	"github.com/parquet-go/parquet-go"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// durabilityTs sits far in the past so every bucket is past end + grace.
const durabilityTs = int64(1_700_000_000_000)

// TestSealCommitAtomicity is the №6 fault injection: a crash between writing
// the class files and the pass commit must not double the bucket's rows on
// retry. Before the fix the first class's RecordSealedFile committed alone and
// the watermark moved in a separate transaction, so a kill -9 in between
// re-sealed the committed rows under a new seq — cross-class duplicates.
func TestSealCommitAtomicity(t *testing.T) {
	ctx := context.Background()
	store, err := Open(Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	key := PodRestartKey{Namespace: "ns", Service: "svc", PodName: "pod-a", RestartTimeMs: 1_000}
	pr, err := store.OpenPodRestart(key)
	require.NoError(t, err)
	redId, err := pr.AppendDictionaryWord(errorMarkerParam)
	require.NoError(t, err)
	// Two classes in one bucket: a clean short call and an errored one, so the
	// pass writes two files and the old bug had a gap to crash into.
	require.NoError(t, pr.AppendCall(durabilityTs, data.Call{
		Method: 1, Duration: 10, ThreadName: "main",
		TraceFileIndex: 1, BufferOffset: 0, RecordIndex: 0,
	}))
	require.NoError(t, pr.AppendCall(durabilityTs+1, data.Call{
		Method: 1, Duration: 10, ThreadName: "main",
		TraceFileIndex: 1, BufferOffset: 100, RecordIndex: 0,
		Params: map[data.TagId][]string{redId: {"1"}},
	}))
	bucket := store.cfg.Bucket(durabilityTs)

	sealBeforeCommit = func() error { return errors.New("injected crash before the seal commit") }
	defer func() { sealBeforeCommit = nil }()
	_, err = store.Seal(ctx, key, bucket)
	require.ErrorContains(t, err, "injected crash")

	files, err := store.LocalParquet(key)
	require.NoError(t, err)
	assert.Empty(t, files, "nothing commits before the pass transaction")
	watermark, err := store.db.SealWatermark(key.String(), bucket)
	require.NoError(t, err)
	assert.Zero(t, watermark, "the watermark cannot outrun the recorded files")

	// The retry re-seals the identical row set under the same deterministic
	// names, replacing the orphan files the crash left behind.
	sealBeforeCommit = nil
	res, err := store.Seal(ctx, key, bucket)
	require.NoError(t, err)
	assert.Equal(t, 2, res.Rows)
	require.Len(t, res.Files, 2)

	files, err = store.LocalParquet(key)
	require.NoError(t, err)
	require.Len(t, files, 2)
	totalRows := 0
	for _, f := range files {
		totalRows += f.RowCount
		assert.Equal(t, 0, f.Seq, "the retry reuses seq 0: no phantom patch files")
	}
	assert.Equal(t, 2, totalRows, "the bucket's row_count must not double on retry")

	var onDisk int
	require.NoError(t, filepath.WalkDir(filepath.Join(store.cfg.DataDir, "parquet"),
		func(path string, d os.DirEntry, err error) error {
			require.NoError(t, err)
			if !d.IsDir() && strings.HasSuffix(path, ".parquet") {
				onDisk++
			}
			return nil
		}))
	assert.Equal(t, 2, onDisk, "the orphans of the crashed pass were replaced, not duplicated")

	sealed, err := store.SealDue(ctx, time.Now().UnixMilli())
	require.NoError(t, err)
	assert.Zero(t, sealed, "the committed watermark covers every indexed call")
}

// bigValuePod seeds one pod-restart with a call whose trace carries a
// PARAM_BIG_DEDUP reference into a sql value segment, the №7 scenario: the
// value exists only in the hot store until the seal inlines it.
func bigValuePod(t *testing.T, store *Store, pod string, tsMs int64) (PodRestartKey, string) {
	t.Helper()
	key := PodRestartKey{Namespace: "ns", Service: "svc", PodName: pod, RestartTimeMs: 1_000}
	pr, err := store.OpenPodRestart(key)
	require.NoError(t, err)
	_, err = pr.AppendDictionaryWord("com.example.Db.query") // method 0
	require.NoError(t, err)
	_, err = pr.AppendDictionaryWord("sql.text") // tag 1
	require.NoError(t, err)

	sqlText := "SELECT * FROM calls WHERE ts > ? ORDER BY ts DESC"
	sqlData, sqlOffs := wire.ValueStream([]string{sqlText})
	sqlSeg, err := pr.OpenSegment(StreamSql, 1)
	require.NoError(t, err)
	_, err = sqlSeg.Write(sqlData)
	require.NoError(t, err)
	require.NoError(t, pr.FinalizeSegment(sqlSeg))

	const threadId = uint64(7)
	stream, offs := wire.TraceStream(500, []wire.TraceChunk{
		{ThreadId: threadId, StartMs: tsMs, Events: []wire.TraceEvent{
			wire.Enter(0, 0), wire.BigTag(0, 1, true, 1, int(sqlOffs[0])), wire.Exit(1),
		}},
	})
	traceSeg, err := pr.OpenSegment(StreamTrace, 1)
	require.NoError(t, err)
	_, err = traceSeg.Write(stream)
	require.NoError(t, err)
	pr.SetTimerStart(500)
	pr.AddChunk(traceSeg, threadId, offs[0], len(stream)-int(offs[0]), tsMs)
	require.NoError(t, pr.FinalizeSegment(traceSeg))

	require.NoError(t, pr.AppendCall(tsMs, data.Call{
		Method: 0, Duration: 10, ThreadName: "main",
		TraceFileIndex: 1, BufferOffset: int(offs[0]), RecordIndex: 0,
	}))
	require.NoError(t, pr.Close())
	return key, sqlText
}

// TestDiskBudgetSparesUnsealedBigValues is the №7 acceptance test: a
// big-dedup call in an unsealed bucket plus a ChunksStagingMaxBytes far below
// the segment total; the janitor pass must evict the sealed fodder FIRST and
// spare the segments the owed seal will read, and the seal must then inline
// the value. Before the fix the sql segment sat in the refcount-0 first tier
// (refcount rises only when a seal commits) and the value vanished with no
// error, reason, or metric.
func TestDiskBudgetSparesUnsealedBigValues(t *testing.T) {
	ctx := context.Background()
	store, err := Open(Config{DataDir: t.TempDir(), ChunksStagingMaxBytes: 4096})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	// Fodder: a closed, fully-sealed pod-restart whose fat incompressible
	// segment is the legitimate eviction victim — dropping it alone satisfies
	// the budget, so the eviction never has to reach the owed-seal tier.
	fodderKey := PodRestartKey{Namespace: "ns", Service: "svc", PodName: "pod-fodder", RestartTimeMs: 1_000}
	fodderPr, err := store.OpenPodRestart(fodderKey)
	require.NoError(t, err)
	fodderSeg, err := fodderPr.OpenSegment(StreamTrace, 1)
	require.NoError(t, err)
	fat := make([]byte, 64<<10)
	_, err = rand.Read(fat)
	require.NoError(t, err)
	_, err = fodderSeg.Write(fat)
	require.NoError(t, err)
	require.NoError(t, fodderPr.FinalizeSegment(fodderSeg))
	require.NoError(t, fodderPr.Close())

	key, sqlText := bigValuePod(t, store, "pod-big", durabilityTs)
	bucket := store.cfg.Bucket(durabilityTs)

	_, err = store.JanitorPass(ctx, time.Now().UnixMilli())
	require.NoError(t, err)

	segs, err := store.Segments(key)
	require.NoError(t, err)
	require.Len(t, segs, 2)
	for _, seg := range segs {
		assert.NotEqual(t, "evicted", seg.Status,
			"%s segment %d holds data an owed seal reads; the disk budget must spare it", seg.Stream, seg.RollingSeq)
		assert.FileExists(t, seg.Path)
	}
	fodderSegs, err := store.Segments(fodderKey)
	require.NoError(t, err)
	require.Len(t, fodderSegs, 1)
	assert.Equal(t, "evicted", fodderSegs[0].Status, "the fully-sealed fodder is the eviction victim")

	res, err := store.Seal(ctx, key, bucket)
	require.NoError(t, err)
	require.Len(t, res.Files, 1)
	assert.Empty(t, res.Truncated, "nothing the seal needed was evicted")
	assert.Zero(t, store.SealCountersSnapshot().LostBigValues)

	rows, err := parquet.ReadFile[storageparquet.CallV2](res.Files[0].Path)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.NotNil(t, rows[0].TraceBlob, "the blob survived")
	require.NotNil(t, rows[0].BigParamsJson, "the big value survived to the cold tier")
	var sealed map[string]string
	require.NoError(t, json.Unmarshal([]byte(*rows[0].BigParamsJson), &sealed))
	assert.Equal(t, map[string]string{"sql:1:0": sqlText}, sealed)
}

// TestSealCountsLostBigValues pins the №7 backstop: when a value segment is
// genuinely gone by seal time, the row seals truncated with disk_budget and
// the loss lands on seal_lost_big_values_total — never a silently-empty
// big_params_json.
func TestSealCountsLostBigValues(t *testing.T) {
	ctx := context.Background()
	store, err := Open(Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	key, _ := bigValuePod(t, store, "pod-lost", durabilityTs)
	bucket := store.cfg.Bucket(durabilityTs)

	// The disk budget won anyway (e.g. a poisoned bucket pinned the unsealed
	// state for days): the sql segment is gone before the seal runs.
	pr, ok := store.PodRestart(key)
	require.True(t, ok)
	require.NoError(t, os.Remove(filepath.Join(pr.dir, StreamSql, SegmentFileName(1))))

	res, err := store.Seal(ctx, key, bucket)
	require.NoError(t, err)
	assert.Equal(t, map[string]int{TruncDiskBudget: 1}, res.Truncated,
		"a lost value truncates its row instead of sealing a silently-empty big_params_json")
	assert.EqualValues(t, 1, store.SealCountersSnapshot().LostBigValues)

	rows, err := parquet.ReadFile[storageparquet.CallV2](res.Files[0].Path)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Nil(t, rows[0].TraceBlob)
	require.NotNil(t, rows[0].TruncatedReason)
	assert.Equal(t, TruncDiskBudget, *rows[0].TruncatedReason)
}

// TestRecoverPurgesIndexRowsPastTruncatedWal is the №8 crash-consistency
// test: a power loss tears calls.wal mid-record while the SQLite index (its
// own file, its own sync policy) kept every row. Recovery must drop the rows
// past the torn tail, and the bucket must seal instead of retrying forever.
func TestRecoverPurgesIndexRowsPastTruncatedWal(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	store, err := Open(Config{DataDir: dataDir})
	require.NoError(t, err)

	key := PodRestartKey{Namespace: "ns", Service: "svc", PodName: "pod-torn", RestartTimeMs: 1_000}
	pr, err := store.OpenPodRestart(key)
	require.NoError(t, err)
	_, err = pr.AppendDictionaryWord("com.example.Api.get")
	require.NoError(t, err)
	for i := 0; i < 3; i++ {
		require.NoError(t, pr.AppendCall(durabilityTs+int64(i), data.Call{
			Method: 0, Duration: 10, ThreadName: "main",
			TraceFileIndex: 1, BufferOffset: i * 100, RecordIndex: 0,
		}))
	}
	bucket := store.cfg.Bucket(durabilityTs)
	rows, err := store.Calls(bucket)
	require.NoError(t, err)
	require.Len(t, rows, 3)
	lastOffset := int64(0)
	for _, r := range rows {
		if r.CallsWalOffset > lastOffset {
			lastOffset = r.CallsWalOffset
		}
	}

	// kill -9: no pod-restart Close, no WAL footers; then the torn tail — the
	// last record loses its body while its index row survives.
	require.NoError(t, store.Close())
	walPath := filepath.Join(pr.dir, "calls.wal")
	require.NoError(t, os.Truncate(walPath, lastOffset+3))

	store, err = Open(Config{DataDir: dataDir})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()
	require.NoError(t, store.Recover(ctx))

	rows, err = store.Calls(bucket)
	require.NoError(t, err)
	assert.Len(t, rows, 2, "the row whose record tore off is dropped with the tail")

	// The bucket seals what survived; before the fix the first pass failed on
	// the missing record and every later pass retried the same failure forever.
	sealed, err := store.SealDue(ctx, time.Now().UnixMilli())
	require.NoError(t, err)
	assert.Equal(t, 1, sealed)
	assert.Zero(t, store.SealSkippedBuckets())
	sealed, err = store.SealDue(ctx, time.Now().UnixMilli())
	require.NoError(t, err)
	assert.Zero(t, sealed, "the watermark covers the survivors; nothing loops")

	recovered, ok := store.PodRestart(key)
	require.True(t, ok)
	files, err := store.LocalParquet(recovered.Key)
	require.NoError(t, err)
	require.Len(t, files, 1)
	assert.Equal(t, 2, files[0].RowCount)
}

// TestSealDueSkipsPoisonedPair pins the №8 loop behaviour: one poisoned
// (pod-restart, bucket) must not starve the others — it is skipped with a
// metric and the pass seals everything else.
func TestSealDueSkipsPoisonedPair(t *testing.T) {
	ctx := context.Background()
	store, err := Open(Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	open := func(pod string) *PodRestart {
		pr, err := store.OpenPodRestart(PodRestartKey{
			Namespace: "ns", Service: "svc", PodName: pod, RestartTimeMs: 1_000})
		require.NoError(t, err)
		_, err = pr.AppendDictionaryWord("com.example.Api.get")
		require.NoError(t, err)
		require.NoError(t, pr.AppendCall(durabilityTs, data.Call{
			Method: 0, Duration: 10, ThreadName: "main",
			TraceFileIndex: 1, BufferOffset: 0, RecordIndex: 0,
		}))
		return pr
	}
	poisoned, healthy := open("pod-poison"), open("pod-ok")
	require.NoError(t, poisoned.Close())
	require.NoError(t, healthy.Close())
	// The poison: the index references records that no longer exist anywhere
	// (a corruption recovery did not see — recovery would have purged it).
	require.NoError(t, os.Remove(filepath.Join(poisoned.dir, "calls.wal")))

	sealed, err := store.SealDue(ctx, time.Now().UnixMilli())
	assert.Error(t, err, "the pass reports the poisoned pair")
	assert.Equal(t, 1, sealed, "the healthy pair seals despite the poisoned one")
	assert.EqualValues(t, 1, store.SealSkippedBuckets())

	files, err := store.LocalParquet(healthy.Key)
	require.NoError(t, err)
	assert.Len(t, files, 1, "the healthy pod-restart's file exists")
}

// TestSealReadsOnlyBucketSlices pins the №9 read pattern: sealing N buckets
// must read about the WAL's size in total — each pass fetches only the
// records its index rows point at — not N × the whole WAL, which is what the
// per-pass os.ReadFile replay cost.
func TestSealReadsOnlyBucketSlices(t *testing.T) {
	ctx := context.Background()
	store, err := Open(Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	key := PodRestartKey{Namespace: "ns", Service: "svc", PodName: "pod-wide", RestartTimeMs: 1_000}
	pr, err := store.OpenPodRestart(key)
	require.NoError(t, err)
	_, err = pr.AppendDictionaryWord("com.example.Api.get")
	require.NoError(t, err)

	// 12 buckets × 20 calls with a fat param ≈ a WAL far larger than any one
	// bucket's slice.
	const buckets, perBucket = 12, 20
	fat := strings.Repeat("x", 4096)
	bucketMs := store.cfg.TimeBucket.Milliseconds()
	for b := 0; b < buckets; b++ {
		for i := 0; i < perBucket; i++ {
			require.NoError(t, pr.AppendCall(durabilityTs+int64(b)*bucketMs+int64(i), data.Call{
				Method: 0, Duration: 10, ThreadName: "main",
				TraceFileIndex: 1, BufferOffset: b*1_000 + i, RecordIndex: 0,
				Params: map[data.TagId][]string{0: {fat}},
			}))
		}
	}
	require.NoError(t, pr.Close())
	walInfo, err := os.Stat(filepath.Join(pr.dir, "calls.wal"))
	require.NoError(t, err)

	sealed, err := store.SealDue(ctx, time.Now().UnixMilli())
	require.NoError(t, err)
	assert.Equal(t, buckets, sealed)

	read := store.SealCountersSnapshot().WalBytesRead
	require.Positive(t, read)
	t.Logf("№9: %d bucket seals read %d of a %d-byte WAL (%.2fx; the old per-pass replay read %dx)",
		buckets, read, walInfo.Size(), float64(read)/float64(walInfo.Size()), buckets)
	assert.Less(t, read, 2*walInfo.Size(),
		"12 bucket seals must read ≈ the WAL once in total (each its own slice), not 12× the whole WAL (old: %d bytes)",
		buckets*walInfo.Size())
}

// TestRenderRowClampsInt32 pins the LOW fix: a >24.8-day duration saturates
// at MaxInt32 instead of wrapping negative in the int32 parquet column.
func TestRenderRowClampsInt32(t *testing.T) {
	store, err := Open(Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()
	pr, err := store.OpenPodRestart(PodRestartKey{
		Namespace: "ns", Service: "svc", PodName: "pod-clamp", RestartTimeMs: 1_000})
	require.NoError(t, err)

	row := &sealRow{
		idx: CallIndexRow{TsMs: durabilityTs},
		wal: CallWalRecord{TsMs: durabilityTs, Call: data.Call{
			Method:            0,
			Duration:          math.MaxInt32 + 100, // ~25 days in ms
			QueueWaitDuration: math.MaxInt32 + 100,
			Calls:             math.MaxInt32 + 100,
		}},
	}
	row.truncated = TruncDiskBudget // no blob needed for the metric columns
	v, err := store.renderRow(pr, row, RetentionLongClean, map[int]string{0: "m"}, 0, false, nil)
	require.NoError(t, err)
	assert.EqualValues(t, math.MaxInt32, v.DurationMs, "a wrapped duration would sort as negative")
	assert.EqualValues(t, math.MaxInt32, v.QueueWaitMs)
	assert.EqualValues(t, math.MaxInt32, v.ChildCalls)
}
