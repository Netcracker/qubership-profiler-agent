package hotstore

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/tests/helpers/wire"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const minute = int64(60_000)

// janitorCallTs sits far in the past so every bucket is past end + grace.
const janitorCallTs = int64(1_700_000_000_000)

// seedSealedCall indexes one already-sealed call of the pod-restart: an index
// row plus a seal watermark covering it, the state a bucket is in once the
// seal loop caught up.
func seedSealedCall(t *testing.T, store *Store, key PodRestartKey, tsMs int64) int64 {
	t.Helper()
	bucket := store.cfg.Bucket(tsMs)
	require.NoError(t, store.db.InsertCall(bucket, CallIndexRow{
		PodRestart: key.String(), TraceFileIndex: 1, BufferOffset: int(tsMs % 100_000), RecordIndex: 0,
		TsMs: tsMs, RetentionClass: RetentionShortClean, CallsWalOffset: 0,
	}))
	require.NoError(t, store.db.UpsertSealState(key.String(), bucket, RetentionShortClean, 1, tsMs))
	return bucket
}

// seedSealedFile records one sealed parquet file with a real file on disk.
func seedSealedFile(t *testing.T, store *Store, key PodRestartKey, bucket int64, seq int) string {
	t.Helper()
	path := filepath.Join(store.cfg.DataDir, fmt.Sprintf("sealed-%d-%d.parquet", bucket, seq))
	require.NoError(t, os.WriteFile(path, []byte("parquet"), 0o644))
	require.NoError(t, store.db.RecordSealedFile(parquetLocalRow{
		Path: path, PodRestart: key.String(), TimeBucketMs: store.cfg.BucketStartMs(bucket),
		RetentionClass: RetentionShortClean, Seq: seq, RowCount: 1,
		TimeMinMs: store.cfg.BucketStartMs(bucket), TimeMaxMs: store.cfg.BucketStartMs(bucket) + 1,
		FileSize: 7, SealedAtMs: janitorCallTs,
		S3Key: fmt.Sprintf("parquet/v1/short_clean/x/%d-%d.parquet", bucket, seq),
	}, nil))
	return path
}

// TestJanitorLifecycle drives one pod-restart through the full hot-store
// lifecycle and pins every gate in order: an unsealed bucket is untouchable, a
// pending upload pins its partition, hot retention holds the partition past
// upload (the §4.3 overlap window), the drop happens only after retention, and
// the WAL purge fires only after the hold-back grace — releasing the in-RAM
// state and the pod-restart directory with it (01 §3.5/§6.3, 03 §3.9 step 18).
func TestJanitorLifecycle(t *testing.T) {
	ctx := context.Background()
	store, err := Open(Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	key := PodRestartKey{Namespace: "ns", Service: "svc", PodName: "pod-a", RestartTimeMs: janitorCallTs}
	pr, err := store.OpenPodRestart(key)
	require.NoError(t, err)
	require.NoError(t, pr.Close()) // closed_at = wall clock now

	now := time.Now().UnixMilli()
	bucket := store.cfg.Bucket(janitorCallTs)
	require.NoError(t, store.db.InsertCall(bucket, CallIndexRow{
		PodRestart: key.String(), TraceFileIndex: 1, BufferOffset: 0, RecordIndex: 0,
		TsMs: janitorCallTs, RetentionClass: RetentionShortClean, CallsWalOffset: 0,
	}))
	walPath := filepath.Join(pr.dir, "dictionary.wal")
	partitionFile := store.db.partitionPath(bucket)
	require.FileExists(t, walPath)
	require.FileExists(t, partitionFile)

	buckets := func() []int64 {
		out, err := store.Buckets()
		require.NoError(t, err)
		return out
	}

	t.Run("an unsealed bucket is never dropped", func(t *testing.T) {
		stats, err := store.JanitorPass(ctx, now+2*60*minute)
		require.NoError(t, err)
		assert.Zero(t, stats.PartitionsDropped)
		assert.Equal(t, []int64{bucket}, buckets())
	})

	require.NoError(t, store.db.UpsertSealState(key.String(), bucket, RetentionShortClean, 1, now))
	sealedPath := seedSealedFile(t, store, key, bucket, 0)

	t.Run("a pending upload pins the partition", func(t *testing.T) {
		stats, err := store.JanitorPass(ctx, now+2*60*minute)
		require.NoError(t, err)
		assert.Zero(t, stats.PartitionsDropped)
		assert.Equal(t, []int64{bucket}, buckets())
		assert.FileExists(t, sealedPath)
	})

	require.NoError(t, store.db.MarkUploaded(sealedPath, now))

	t.Run("inside hot retention both tiers hold the rows", func(t *testing.T) {
		stats, err := store.JanitorPass(ctx, now+5*minute)
		require.NoError(t, err)
		assert.Zero(t, stats.ParquetDeleted)
		assert.Zero(t, stats.PartitionsDropped)
		assert.Equal(t, []int64{bucket}, buckets())
		assert.FileExists(t, sealedPath)
	})

	t.Run("past hot retention the bucket leaves the hot tier", func(t *testing.T) {
		stats, err := store.JanitorPass(ctx, now+16*minute)
		require.NoError(t, err)
		assert.EqualValues(t, 1, stats.ParquetDeleted)
		assert.EqualValues(t, 1, stats.PartitionsDropped)
		assert.Zero(t, stats.WalsPurged, "the purge grace has not elapsed")
		assert.Empty(t, buckets())
		assert.NoFileExists(t, sealedPath)
		assert.NoFileExists(t, partitionFile)
		assert.FileExists(t, walPath, "WALs survive until the purge grace")
		_, ok := store.PodRestart(key)
		assert.True(t, ok, "in-RAM state survives with the WALs")
	})

	t.Run("past the grace the WALs and the pod-restart dir go", func(t *testing.T) {
		stats, err := store.JanitorPass(ctx, now+2*60*minute)
		require.NoError(t, err)
		assert.EqualValues(t, 1, stats.WalsPurged)
		assert.NoFileExists(t, walPath)
		assert.NoDirExists(t, pr.dir)
		assert.NoDirExists(t, filepath.Join(store.cfg.DataDir, "pods", "ns"),
			"empty namespace/service/pod parents are removed")
		_, ok := store.PodRestart(key)
		assert.False(t, ok, "in-RAM state released with the WALs")

		candidates, err := store.db.WalPurgeCandidates()
		require.NoError(t, err)
		assert.Empty(t, candidates, "wals_purged_at takes the pod-restart out of the purge queue")
	})

	t.Run("a repeated pass is a no-op", func(t *testing.T) {
		stats, err := store.JanitorPass(ctx, now+3*60*minute)
		require.NoError(t, err)
		assert.Equal(t, JanitorStats{}, stats)
	})
}

// seedFastPurgeCandidate builds the 03 §3.9 step-18a shape: a closed, tiny
// pod-restart with one dictionary word, one sealed-and-uploaded call still
// indexed in a live partition (the recent upload keeps the partition inside
// hot retention, so gate (b) holds). Returns the pod-restart and the pass
// clock at which the grace has elapsed but the partition has not dropped.
func seedFastPurgeCandidate(t *testing.T, store *Store, key PodRestartKey, word string) (pr *PodRestart, passNowMs int64) {
	t.Helper()
	pr, err := store.OpenPodRestart(key)
	require.NoError(t, err)
	wordId, err := pr.AppendDictionaryWord(word)
	require.NoError(t, err)
	require.NoError(t, pr.Close()) // closed_at = wall clock now; dictionary unloads

	now := time.Now().UnixMilli()
	passNowMs = now + 2*60*minute // past the 1 h purge grace
	bucket := store.cfg.Bucket(janitorCallTs)
	require.NoError(t, store.db.InsertCall(bucket, CallIndexRow{
		PodRestart: pr.Key.String(), TraceFileIndex: 1, BufferOffset: 0, RecordIndex: 0,
		TsMs: janitorCallTs, MethodId: wordId, RetentionClass: RetentionShortClean, CallsWalOffset: 0,
	}))
	require.NoError(t, store.db.UpsertSealState(pr.Key.String(), bucket, RetentionShortClean, 1, now))
	sealedPath := seedSealedFile(t, store, pr.Key, bucket, 0)
	// Uploaded 5 minutes before the pass clock: inside hot retention, so the
	// partition (and gate (b)) stays put while the grace is long gone.
	require.NoError(t, store.db.MarkUploaded(sealedPath, passNowMs-5*minute))
	return pr, passNowMs
}

// TestFastPurgeNearEmptyPodRestart pins the 03 §3.9 step-18a fast path: a
// near-empty, fully sealed and uploaded pod-restart purges after the grace
// even though its rows are still indexed in a live partition. The rows keep
// rendering: method_text is backfilled from the dictionary (lazy-reloaded
// from dictionary.wal — Close unloaded the in-RAM maps) before the WAL goes.
func TestFastPurgeNearEmptyPodRestart(t *testing.T) {
	ctx := context.Background()
	store, err := Open(Config{DataDir: t.TempDir(), WalPurgeFastMaxBytes: 1 << 20})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	key := PodRestartKey{Namespace: "ns", Service: "svc", PodName: "pod-f", RestartTimeMs: janitorCallTs}
	pr, passNow := seedFastPurgeCandidate(t, store, key, "com.example.Foo.bar()")
	bucket := store.cfg.Bucket(janitorCallTs)
	walPath := filepath.Join(pr.dir, "dictionary.wal")

	stats, err := store.JanitorPass(ctx, passNow)
	require.NoError(t, err)
	assert.EqualValues(t, 1, stats.WalsPurged, "the fast path is a purge")
	assert.EqualValues(t, 1, stats.WalsFastPurged)
	assert.Zero(t, stats.PartitionsDropped, "hot retention still pins the partition")
	assert.NoFileExists(t, walPath)
	assert.NoDirExists(t, pr.dir)
	_, tracked := store.PodRestart(pr.Key)
	assert.False(t, tracked, "the pod-restart leaves the tracked set at the grace, not at the partition drop")

	lifetime := store.JanitorCountersSnapshot()
	assert.EqualValues(t, 1, lifetime.WalsFastPurged, "countJanitor must accumulate the new counter")

	rows, err := store.Calls(bucket)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, "com.example.Foo.bar()", rows[0].MethodText,
		"method_text is materialized before the dictionary WAL goes")

	candidates, err := store.db.WalPurgeCandidates()
	require.NoError(t, err)
	assert.Empty(t, candidates)

	// The partition still drops on its own schedule once retention passes.
	stats, err = store.JanitorPass(ctx, passNow+20*minute)
	require.NoError(t, err)
	assert.EqualValues(t, 1, stats.PartitionsDropped)
	stats, err = store.JanitorPass(ctx, passNow+30*minute)
	require.NoError(t, err)
	assert.Equal(t, JanitorStats{}, stats, "a repeated pass is a no-op")
}

// TestFastPurgeGates pins every blocker of the step-18a fast path: the
// disabled knob, a directory over the floor, an unsealed call, a pending
// (quarantined) upload, and the not-yet-elapsed grace all keep the WAL set.
func TestFastPurgeGates(t *testing.T) {
	ctx := context.Background()

	t.Run("zero floor disables the fast path", func(t *testing.T) {
		store, err := Open(Config{DataDir: t.TempDir()})
		require.NoError(t, err)
		defer func() { _ = store.Close() }()
		key := PodRestartKey{Namespace: "ns", Service: "svc", PodName: "pod-g0", RestartTimeMs: janitorCallTs}
		pr, passNow := seedFastPurgeCandidate(t, store, key, "w")
		stats, err := store.JanitorPass(ctx, passNow)
		require.NoError(t, err)
		assert.Zero(t, stats.WalsPurged)
		assert.FileExists(t, filepath.Join(pr.dir, "dictionary.wal"))
	})

	t.Run("a directory over the floor keeps the regular path", func(t *testing.T) {
		store, err := Open(Config{DataDir: t.TempDir(), WalPurgeFastMaxBytes: 1 << 20})
		require.NoError(t, err)
		defer func() { _ = store.Close() }()
		key := PodRestartKey{Namespace: "ns", Service: "svc", PodName: "pod-g1", RestartTimeMs: janitorCallTs}
		pr, passNow := seedFastPurgeCandidate(t, store, key, "w")
		require.NoError(t, os.WriteFile(filepath.Join(pr.dir, StreamTrace, SegmentFileName(1)),
			bytes.Repeat([]byte{0xAB}, 2<<20), 0o644))
		stats, err := store.JanitorPass(ctx, passNow)
		require.NoError(t, err)
		assert.Zero(t, stats.WalsPurged)
		assert.FileExists(t, filepath.Join(pr.dir, "dictionary.wal"))
	})

	t.Run("an unsealed call blocks the fast path", func(t *testing.T) {
		store, err := Open(Config{DataDir: t.TempDir(), WalPurgeFastMaxBytes: 1 << 20})
		require.NoError(t, err)
		defer func() { _ = store.Close() }()
		key := PodRestartKey{Namespace: "ns", Service: "svc", PodName: "pod-g2", RestartTimeMs: janitorCallTs}
		pr, passNow := seedFastPurgeCandidate(t, store, key, "w")
		require.NoError(t, store.db.InsertCall(store.cfg.Bucket(janitorCallTs), CallIndexRow{
			PodRestart: pr.Key.String(), TraceFileIndex: 2, BufferOffset: 0, RecordIndex: 0,
			TsMs: janitorCallTs + 1, RetentionClass: RetentionShortClean, CallsWalOffset: 7, // >= watermark 1
		}))
		stats, err := store.JanitorPass(ctx, passNow)
		require.NoError(t, err)
		assert.Zero(t, stats.WalsPurged)
		assert.FileExists(t, filepath.Join(pr.dir, "dictionary.wal"))
	})

	t.Run("a quarantined upload blocks any purge", func(t *testing.T) {
		store, err := Open(Config{DataDir: t.TempDir(), WalPurgeFastMaxBytes: 1 << 20})
		require.NoError(t, err)
		defer func() { _ = store.Close() }()
		key := PodRestartKey{Namespace: "ns", Service: "svc", PodName: "pod-g3", RestartTimeMs: janitorCallTs}
		pr, passNow := seedFastPurgeCandidate(t, store, key, "w")
		quarantined := seedSealedFile(t, store, pr.Key, store.cfg.Bucket(janitorCallTs), 1)
		require.NoError(t, store.db.MarkUploadFailed(quarantined, quarantined+".failed", passNow))
		stats, err := store.JanitorPass(ctx, passNow)
		require.NoError(t, err)
		assert.Zero(t, stats.WalsPurged)
		assert.FileExists(t, filepath.Join(pr.dir, "dictionary.wal"))
	})

	t.Run("the grace is never skipped", func(t *testing.T) {
		store, err := Open(Config{DataDir: t.TempDir(), WalPurgeFastMaxBytes: 1 << 20})
		require.NoError(t, err)
		defer func() { _ = store.Close() }()
		key := PodRestartKey{Namespace: "ns", Service: "svc", PodName: "pod-g4", RestartTimeMs: janitorCallTs}
		pr, _ := seedFastPurgeCandidate(t, store, key, "w")
		stats, err := store.JanitorPass(ctx, time.Now().UnixMilli()+minute)
		require.NoError(t, err)
		assert.Zero(t, stats.WalsPurged)
		assert.FileExists(t, filepath.Join(pr.dir, "dictionary.wal"))
	})
}

// TestFastPurgeCrashRetryConverges pins the step-18a idempotence: a purge that
// crashed after the backfill and the WAL deletion but before SetWalsPurged
// re-runs to completion — the size check reads a missing directory as zero,
// the backfill is a no-op (no NULL rows are left), and the names survive.
func TestFastPurgeCrashRetryConverges(t *testing.T) {
	ctx := context.Background()
	store, err := Open(Config{DataDir: t.TempDir(), WalPurgeFastMaxBytes: 1 << 20})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	key := PodRestartKey{Namespace: "ns", Service: "svc", PodName: "pod-c", RestartTimeMs: janitorCallTs}
	pr, passNow := seedFastPurgeCandidate(t, store, key, "com.example.Crash.mid()")
	bucket := store.cfg.Bucket(janitorCallTs)

	// The first attempt got exactly as far as the durable steps: method_text
	// committed, files gone — and crashed before SetWalsPurged.
	unresolved, err := store.db.BackfillMethodText(pr.Key.String(), pr.DictWord)
	require.NoError(t, err)
	require.Zero(t, unresolved)
	require.NoError(t, os.RemoveAll(pr.dir))

	stats, err := store.JanitorPass(ctx, passNow)
	require.NoError(t, err)
	assert.EqualValues(t, 1, stats.WalsPurged)
	assert.EqualValues(t, 1, stats.WalsFastPurged)
	rows, err := store.Calls(bucket)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, "com.example.Crash.mid()", rows[0].MethodText, "the retry must not lose the backfilled names")
	candidates, err := store.db.WalPurgeCandidates()
	require.NoError(t, err)
	assert.Empty(t, candidates, "the retry completes the purge")
}

// TestFastPurgeSurvivesRecovery pins the post-purge restart shape: the
// pod-restart directory is gone, so recovery (which walks the PV, 03 §3.3)
// must not resurrect the pod-restart, while its still-indexed rows keep their
// backfilled names — they live in the partition, not in the purged state.
func TestFastPurgeSurvivesRecovery(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	store, err := Open(Config{DataDir: dataDir, WalPurgeFastMaxBytes: 1 << 20})
	require.NoError(t, err)

	key := PodRestartKey{Namespace: "ns", Service: "svc", PodName: "pod-v", RestartTimeMs: janitorCallTs}
	pr, passNow := seedFastPurgeCandidate(t, store, key, "com.example.Verify.run()")
	bucket := store.cfg.Bucket(janitorCallTs)
	stats, err := store.JanitorPass(ctx, passNow)
	require.NoError(t, err)
	require.EqualValues(t, 1, stats.WalsFastPurged)
	require.NoError(t, store.Close())

	store, err = Open(Config{DataDir: dataDir, WalPurgeFastMaxBytes: 1 << 20})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()
	require.NoError(t, store.Recover(ctx))

	_, tracked := store.PodRestart(pr.Key)
	assert.False(t, tracked, "recovery must not resurrect a purged pod-restart")
	rows, err := store.Calls(bucket)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, "com.example.Verify.run()", rows[0].MethodText, "the names live in the partition")
}

// TestJanitorQuarantineBlocksPartitionDrops pins the contiguity barrier: a
// bucket whose parquet is quarantined (not durable in S3) keeps ITS partition
// and every newer partition in the hot tier, however aged, so the hot window
// stays truthful and the query's cold cutoff never skips rows that exist
// nowhere in S3 (02 §4.3 zero-gap).
func TestJanitorQuarantineBlocksPartitionDrops(t *testing.T) {
	ctx := context.Background()
	store, err := Open(Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	key := PodRestartKey{Namespace: "ns", Service: "svc", PodName: "pod-q", RestartTimeMs: janitorCallTs}
	require.NoError(t, store.db.UpsertPodRestart(key, janitorCallTs))

	now := time.Now().UnixMilli()
	bucket1 := seedSealedCall(t, store, key, janitorCallTs)
	bucket2 := seedSealedCall(t, store, key, janitorCallTs+2*store.cfg.TimeBucket.Milliseconds())
	require.Less(t, bucket1, bucket2)

	// Bucket 1's file is quarantined: uploaded_at stays NULL forever.
	quarantined := seedSealedFile(t, store, key, bucket1, 0)
	require.NoError(t, store.db.MarkUploadFailed(quarantined, quarantined+".failed", now))
	// Bucket 2's file is uploaded long ago: droppable on its own merits.
	uploaded := seedSealedFile(t, store, key, bucket2, 0)
	require.NoError(t, store.db.MarkUploaded(uploaded, now-60*minute))

	stats, err := store.JanitorPass(ctx, now)
	require.NoError(t, err)
	assert.EqualValues(t, 1, stats.ParquetDeleted, "the aged LOCAL file of bucket 2 still goes (§6.3)")
	assert.Zero(t, stats.PartitionsDropped,
		"bucket 1 is not durable in S3, so neither partition may leave the hot index")
	buckets, err := store.Buckets()
	require.NoError(t, err)
	assert.Equal(t, []int64{bucket1, bucket2}, buckets)

	// A human resolves the rejection: the copy is now durable in S3.
	require.NoError(t, store.db.meta.Exec(
		`UPDATE parquet_local SET uploaded_at = ?, upload_failed_at = NULL WHERE path = ?`,
		now-60*minute, quarantined+".failed").Error)

	stats, err = store.JanitorPass(ctx, now)
	require.NoError(t, err)
	assert.EqualValues(t, 2, stats.PartitionsDropped, "both buckets drop once every copy is durable")
	buckets, err = store.Buckets()
	require.NoError(t, err)
	assert.Empty(t, buckets)
}

// TestJanitorEvictionOrder pins the §4.6 disk-budget policy: refcount-0
// segments go first (oldest created first), referenced segments only if the
// budget still overflows, and a segment open for writes is never touched. The
// evicted row keeps its refcount and turns 'evicted', which is exactly what
// the seal pass maps to truncated_reason = disk_budget.
func TestJanitorEvictionOrder(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	key := PodRestartKey{Namespace: "ns", Service: "svc", PodName: "pod-e", RestartTimeMs: janitorCallTs}

	openStore := func(budget int64) *Store {
		store, err := Open(Config{DataDir: dataDir, ChunksStagingMaxBytes: budget})
		require.NoError(t, err)
		return store
	}
	store := openStore(250)
	require.NoError(t, store.db.UpsertPodRestart(key, janitorCallTs))
	// A LIVE pod-restart's segments sit in the evicted-last tier (finding 5);
	// this test models the sealed ballast of a closed one.
	require.NoError(t, store.db.ClosePodRestart(key, janitorCallTs))

	seed := func(seq int, createdAtMs int64, refcount int, finalize bool) string {
		path := filepath.Join(dataDir, fmt.Sprintf("seg-%06d.gz", seq))
		require.NoError(t, os.WriteFile(path, bytes.Repeat([]byte{0xAB}, 100), 0o644))
		require.NoError(t, store.db.UpsertSegment(key.String(), StreamTrace, seq, path, createdAtMs))
		if finalize {
			require.NoError(t, store.db.FinalizeSegment(key.String(), StreamTrace, seq, 100, nil, nil))
		}
		if refcount > 0 {
			require.NoError(t, store.db.meta.Exec(`UPDATE segments SET refcount = ?
				WHERE pod_restart = ? AND stream = ? AND rolling_seq = ?`,
				refcount, key.String(), StreamTrace, seq).Error)
		}
		return path
	}
	// C is the OLDEST but referenced; A and B are refcount-0. D is still open.
	segC := seed(3, 50, 5, true)
	segA := seed(1, 100, 0, true)
	segB := seed(2, 200, 0, true)
	segD := seed(4, 25, 0, false)

	status := func(seq int) string {
		rows, err := store.db.Segments(key.String())
		require.NoError(t, err)
		for _, r := range rows {
			if r.RollingSeq == seq {
				return r.Status
			}
		}
		return "missing"
	}

	// 400 bytes over a 250 budget: the two refcount-0 closed segments go, in
	// created_at order, and that is enough — C (referenced) and D (open) stay.
	stats, err := store.JanitorPass(ctx, janitorCallTs)
	require.NoError(t, err)
	assert.EqualValues(t, 2, stats.SegmentsEvicted)
	assert.EqualValues(t, 200, stats.EvictedBytes)
	assert.NoFileExists(t, segA)
	assert.NoFileExists(t, segB)
	assert.FileExists(t, segC, "a referenced segment survives while refcount-0 ones suffice")
	assert.FileExists(t, segD, "an open segment is never evicted")
	assert.Equal(t, "evicted", status(1))
	assert.Equal(t, "evicted", status(2))

	// 200 bytes over a 120 budget with no refcount-0 candidates left: the
	// referenced segment is evicted too, refcount intact; the open one never.
	require.NoError(t, store.Close())
	store = openStore(120)
	defer func() { _ = store.Close() }()
	stats, err = store.JanitorPass(ctx, janitorCallTs)
	require.NoError(t, err)
	assert.EqualValues(t, 1, stats.SegmentsEvicted)
	assert.NoFileExists(t, segC)
	assert.FileExists(t, segD)
	assert.Equal(t, "evicted", status(3))
	rows, err := store.db.Segments(key.String())
	require.NoError(t, err)
	for _, r := range rows {
		if r.RollingSeq == 3 {
			assert.Equal(t, 5, r.Refcount, "eviction must not touch the refcount (the upload releases it)")
		}
	}
}

// TestDiskBudgetSparesLivePodValueSegments pins the finding-5 fix: the agent
// deduplicates sql/xml values per CONNECTION, so a PARAM_BIG_DEDUP tag can
// reference a value sent hours ago; while the connection is live, its rotated
// value segments look refcount-0 (the refcount rises only when a referencing
// call seals) and used to fall into the first-evicted tier. A live
// pod-restart's segments must outlive the sealed ballast of closed ones, and
// a later dedup reference must still resolve.
func TestDiskBudgetSparesLivePodValueSegments(t *testing.T) {
	ctx := context.Background()
	store, err := Open(Config{DataDir: t.TempDir(), ChunksStagingMaxBytes: 1000})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	// The live pod-restart writes one value into a rotated sql segment.
	liveKey := PodRestartKey{Namespace: "ns", Service: "svc", PodName: "pod-live", RestartTimeMs: janitorCallTs}
	pr, err := store.OpenPodRestart(liveKey)
	require.NoError(t, err)
	valueBytes, offsets := wire.ValueStream([]string{"SELECT * FROM dedup"})
	seg, err := pr.OpenSegment(StreamSql, 1)
	require.NoError(t, err)
	_, err = seg.Write(valueBytes)
	require.NoError(t, err)
	require.NoError(t, pr.FinalizeSegment(seg)) // rotated: closed for writes, still refcount-0
	sqlPath := filepath.Join(pr.dir, StreamSql, SegmentFileName(1))
	require.FileExists(t, sqlPath)

	// Closed-pod ballast, refcount 0 and created strictly LATER than the live
	// segment. Pre-fix, both fell into zeroRef and were evicted oldest first,
	// so the LIVE segment went before the ballast — exactly the regression
	// this ordering makes the test catch.
	closedKey := PodRestartKey{Namespace: "ns", Service: "svc", PodName: "pod-closed", RestartTimeMs: janitorCallTs}
	require.NoError(t, store.db.UpsertPodRestart(closedKey, janitorCallTs))
	require.NoError(t, store.db.ClosePodRestart(closedKey, janitorCallTs))
	ballast := filepath.Join(store.cfg.DataDir, "ballast.gz")
	require.NoError(t, os.WriteFile(ballast, bytes.Repeat([]byte{0xAB}, 10_000), 0o644))
	require.NoError(t, store.db.UpsertSegment(closedKey.String(), StreamTrace, 1, ballast, time.Now().UnixMilli()+minute))
	require.NoError(t, store.db.FinalizeSegment(closedKey.String(), StreamTrace, 1, 10_000, nil, nil))

	stats, err := store.JanitorPass(ctx, time.Now().UnixMilli())
	require.NoError(t, err)
	assert.EqualValues(t, 1, stats.SegmentsEvicted, "the ballast alone satisfies the budget")
	assert.NoFileExists(t, ballast, "the closed pod's refcount-0 ballast goes first")
	assert.FileExists(t, sqlPath, "a live pod's value segment must not be evicted ahead of sealed ballast")

	// The dedup reference from a call arriving AFTER the pass still resolves.
	values, err := store.BigValues(ctx, liveKey, []ValueRef{{Stream: StreamSql, Seq: 1, Offset: offsets[0]}})
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM dedup",
		values[ValueRef{Stream: StreamSql, Seq: 1, Offset: offsets[0]}],
		"a later dedup reference must resolve against the protected segment")
}

// TestJanitorSweepsOrphanSealedParquet pins the finding-7 fix: a crash between
// the seal rename and CommitSealPass leaves a sealed file with no catalog row —
// invisible to the pending backlog and to every catalog-keyed janitor step.
// The janitor sweep must remove it without waiting for a restart, while
// sparing catalogued files and files young enough to be a pass mid-commit.
func TestJanitorSweepsOrphanSealedParquet(t *testing.T) {
	ctx := context.Background()
	store, err := Open(Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()
	key := PodRestartKey{Namespace: "ns", Service: "svc", PodName: "pod-o", RestartTimeMs: janitorCallTs}
	require.NoError(t, store.db.UpsertPodRestart(key, janitorCallTs))

	dir := filepath.Join(store.cfg.DataDir, "parquet", "v1", "short_clean", "2026", "01", "01", "00")
	require.NoError(t, os.MkdirAll(dir, 0o755))
	now := time.Now()
	aged := now.Add(-time.Hour)
	write := func(name string, mtime time.Time) string {
		path := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(path, []byte("parquet"), 0o644))
		require.NoError(t, os.Chtimes(path, mtime, mtime))
		return path
	}
	orphanOld := write("orphan-old.parquet", aged)
	orphanFresh := write("orphan-fresh.parquet", now)
	catalogued := write("catalogued.parquet", aged)
	require.NoError(t, store.db.RecordSealedFile(parquetLocalRow{
		Path: catalogued, PodRestart: key.String(), TimeBucketMs: 0,
		RetentionClass: RetentionShortClean, Seq: 0, RowCount: 1,
		TimeMinMs: 1, TimeMaxMs: 2, FileSize: 7, SealedAtMs: janitorCallTs,
		S3Key: "parquet/v1/short_clean/2026/01/01/00/catalogued.parquet",
	}, nil))

	stats, err := store.JanitorPass(ctx, now.UnixMilli())
	require.NoError(t, err)
	assert.EqualValues(t, 1, stats.OrphanParquetRemoved)
	assert.NoFileExists(t, orphanOld, "an aged catalog-less file is a crash leftover and goes now, not at restart")
	assert.FileExists(t, orphanFresh, "a fresh catalog-less file may be a seal pass mid-commit")
	assert.FileExists(t, catalogued, "a catalogued file is never an orphan")
}

// TestPartitionResurrectOnLateInsert pins the dropped-bucket escape hatch: a
// very late Call whose bucket was already dropped must re-create the partition
// AND clear dropped_at, or its row would be invisible to the seal loop and
// every reader — an unrecoverable data hole.
func TestPartitionResurrectOnLateInsert(t *testing.T) {
	ctx := context.Background()
	store, err := Open(Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	key := PodRestartKey{Namespace: "ns", Service: "svc", PodName: "pod-r", RestartTimeMs: janitorCallTs}
	require.NoError(t, store.db.UpsertPodRestart(key, janitorCallTs))
	bucket := seedSealedCall(t, store, key, janitorCallTs)

	now := time.Now().UnixMilli()
	stats, err := store.JanitorPass(ctx, now)
	require.NoError(t, err)
	require.EqualValues(t, 1, stats.PartitionsDropped)
	buckets, err := store.Buckets()
	require.NoError(t, err)
	require.Empty(t, buckets)

	require.NoError(t, store.db.InsertCall(bucket, CallIndexRow{
		PodRestart: key.String(), TraceFileIndex: 2, BufferOffset: 0, RecordIndex: 0,
		TsMs: janitorCallTs + 1, RetentionClass: RetentionShortClean, CallsWalOffset: 7,
	}))
	buckets, err = store.Buckets()
	require.NoError(t, err)
	assert.Equal(t, []int64{bucket}, buckets, "the late row resurrects its bucket")
	rows, err := store.Calls(bucket)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.EqualValues(t, 7, rows[0].CallsWalOffset)

	// The resurrected bucket is unsealed again (offset 7 >= watermark 1), so
	// the janitor leaves it for the seal loop.
	stats, err = store.JanitorPass(ctx, now)
	require.NoError(t, err)
	assert.Zero(t, stats.PartitionsDropped)
}
