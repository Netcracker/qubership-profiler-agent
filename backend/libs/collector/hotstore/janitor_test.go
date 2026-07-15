package hotstore

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

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
	require.NoError(t, store.db.SetDictUploaded(key.String(), now))

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
