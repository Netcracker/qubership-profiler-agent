package hotstore

// №26 recovery robustness: one broken pod-restart quarantines instead of
// crash-looping the collector, and a crash between the seal rename and the
// pass commit leaves no orphan sealed files behind. The crashes are genuine:
// the store closes without footering the WALs and the corruption is injected
// into the on-disk bytes.

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/protocol/data"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRecoverQuarantinesCorruptPodRestart pins the 03 §4 "degrade, not fail"
// behaviour: a pod-restart whose dictionary.wal replays into garbage — a
// structurally valid record with an undecodable body, the kind of damage a
// torn page leaves behind — is quarantined under recovery-failed/ while every
// other pod-restart recovers and the collector starts.
func TestRecoverQuarantinesCorruptPodRestart(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	store, err := Open(Config{DataDir: dataDir})
	require.NoError(t, err)

	open := func(pod string) *PodRestart {
		pr, err := store.OpenPodRestart(PodRestartKey{
			Namespace: "ns", Service: "svc", PodName: pod, RestartTimeMs: 1_000})
		require.NoError(t, err)
		_, err = pr.AppendDictionaryWord("com.example.Api.get")
		require.NoError(t, err)
		require.NoError(t, pr.AppendCall(1_700_000_000_000, data.Call{
			Method: 0, Duration: 10, ThreadName: "main",
			TraceFileIndex: 1, BufferOffset: 0, RecordIndex: 0,
		}))
		return pr
	}
	corrupt, healthy := open("pod-corrupt"), open("pod-ok")
	corruptDir, healthyKey := corrupt.dir, healthy.Key
	bucket := store.cfg.Bucket(1_700_000_000_000)

	// kill -9, then the damage: a record whose framing and CRC are fine but
	// whose body is not a dictionary entry, so the replay hard-fails.
	require.NoError(t, store.Close())
	w, err := OpenWal(filepath.Join(corruptDir, "dictionary.wal"), 1, time.Millisecond)
	require.NoError(t, err)
	_, err = w.Append([]byte{0xFF}) // a lone continuation byte: no valid varint
	require.NoError(t, err)
	require.NoError(t, w.Sync())

	store, err = Open(Config{DataDir: dataDir})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()
	require.NoError(t, store.Recover(ctx), "one broken pod-restart must not fail recovery")

	_, ok := store.PodRestart(PodRestartKey{
		Namespace: "ns", Service: "svc", PodName: "pod-corrupt", RestartTimeMs: 1_000})
	assert.False(t, ok, "the broken pod-restart is not resurrected")
	assert.NoDirExists(t, corruptDir, "its directory left the pods/ tree")
	assert.DirExists(t, filepath.Join(dataDir, "recovery-failed", "ns_svc_pod-corrupt_1000"),
		"the directory waits under recovery-failed/ for a human")

	rows, err := store.Calls(bucket)
	require.NoError(t, err)
	require.Len(t, rows, 1, "the quarantined pod-restart's index rows are purged")
	assert.Equal(t, healthyKey.String(), rows[0].PodRestart)

	_, ok = store.PodRestart(healthyKey)
	assert.True(t, ok, "the healthy pod-restart recovered")
	sealed, err := store.SealDue(ctx, time.Now().UnixMilli())
	require.NoError(t, err)
	assert.Equal(t, 1, sealed, "the healthy bucket seals; nothing waits on the quarantined one")
}

// TestRecoverRemovesOrphanSealedParquet pins the №6 crash-window cleanup: a
// sealed file with no parquet_local row (kill -9 between the rename and the
// pass commit) is swept on recovery, while catalogued files stay.
func TestRecoverRemovesOrphanSealedParquet(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	store, err := Open(Config{DataDir: dataDir})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	key := PodRestartKey{Namespace: "ns", Service: "svc", PodName: "pod-o", RestartTimeMs: 1_000}
	require.NoError(t, store.db.UpsertPodRestart(key, 1_000))

	sealedDir := filepath.Join(dataDir, "parquet", "v1", "short_clean", "2023", "11", "14", "22")
	require.NoError(t, os.MkdirAll(sealedDir, 0o755))
	catalogued := filepath.Join(sealedDir, "collector-0-aaaa-x-x-x-0.parquet")
	orphan := filepath.Join(sealedDir, "collector-0-bbbb-x-x-x-0.parquet")
	for _, p := range []string{catalogued, orphan} {
		require.NoError(t, os.WriteFile(p, []byte("parquet"), 0o644))
	}
	require.NoError(t, store.db.RecordSealedFile(parquetLocalRow{
		Path: catalogued, PodRestart: key.String(), TimeBucketMs: 0,
		RetentionClass: RetentionShortClean, Seq: 0, RowCount: 1,
		TimeMinMs: 1, TimeMaxMs: 2, FileSize: 7, SealedAtMs: 1,
		S3Key: "parquet/v1/short_clean/x/a.parquet",
	}, nil))

	require.NoError(t, store.Recover(ctx))
	assert.FileExists(t, catalogued, "a catalogued file survives recovery")
	assert.NoFileExists(t, orphan, "an uncommitted seal's file is swept")
}

// TestRecoverSweepsOrphanQuarantinedParquet pins issue #824: quarantine() renames
// a rejected parquet into upload-failed/ before it records the move, so a rename
// that succeeds followed by a MarkUploadFailed that fails leaves the file under
// upload-failed/ with no owning parquet_local row. The quarantine cap and the
// upload loop both walk rows only, so nothing but recovery can reclaim it.
// Recovery sweeps upload-failed/ the same way it sweeps parquet/: a .parquet file
// catalogued by no row is removed, while a legitimately quarantined file — one
// whose row points at its upload-failed/ path — survives.
func TestRecoverSweepsOrphanQuarantinedParquet(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	store, err := Open(Config{DataDir: dataDir})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	key := PodRestartKey{Namespace: "ns", Service: "svc", PodName: "pod-q", RestartTimeMs: 1_000}
	require.NoError(t, store.db.UpsertPodRestart(key, 1_000))

	uploadFailedDir := filepath.Join(dataDir, "upload-failed")
	seed := func(s3Key string) string {
		path := filepath.Join(uploadFailedDir, filepath.FromSlash(s3Key))
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, []byte("parquet"), 0o644))
		return path
	}

	// The double-fault orphan: the file moved but MarkUploadFailed never
	// committed, so no row owns it.
	orphan := seed("parquet/v1/short_clean/2023/11/14/22/collector-0-bbbb-x-x-x-0.parquet")

	// A legitimately quarantined file: its row points at the upload-failed/ path,
	// so the sweep must spare it.
	quarantined := seed("parquet/v1/short_clean/2023/11/14/22/collector-0-aaaa-x-x-x-0.parquet")
	require.NoError(t, store.db.RecordSealedFile(parquetLocalRow{
		Path: quarantined, PodRestart: key.String(), TimeBucketMs: 0,
		RetentionClass: RetentionShortClean, Seq: 0, RowCount: 1,
		TimeMinMs: 1, TimeMaxMs: 2, FileSize: 7, SealedAtMs: 1,
		S3Key: "parquet/v1/short_clean/x/a.parquet",
	}, nil))
	require.NoError(t, store.db.MarkUploadFailed(quarantined, quarantined, 1))

	require.NoError(t, store.Recover(ctx))
	assert.NoFileExists(t, orphan, "an orphan with no parquet_local row is swept")
	assert.FileExists(t, quarantined, "a legitimately quarantined file keeps its row and survives")
}

// TestRecoverReSealsLostPendingParquet pins the QA 708#2 fix: a pending (not yet
// uploaded) sealed file whose local copy vanished before it reached S3 must not
// be silently forgotten. Recovery rewinds the bucket's seal watermark to the
// pass that produced it, so its source calls re-seal into a replacement instead
// of being stranded below the watermark forever. On the pre-fix drop-only path
// the calls stayed sealed-and-below-watermark, HasUnsealedCalls returned false,
// and the bucket never re-sealed — durable data loss.
func TestRecoverReSealsLostPendingParquet(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	store, err := Open(Config{DataDir: dataDir})
	require.NoError(t, err)

	key := PodRestartKey{Namespace: "ns", Service: "svc", PodName: "pod", RestartTimeMs: 1_000}
	pr, err := store.OpenPodRestart(key)
	require.NoError(t, err)

	baseMs := int64(1_700_000_000_000)
	for i, off := range []int{0, 100} {
		require.NoError(t, pr.AppendCall(baseMs+int64(i), data.Call{
			Method: 0, Duration: 10, ThreadName: "main",
			TraceFileIndex: 1, BufferOffset: off, RecordIndex: 0,
		}))
	}

	cfg := store.Config()
	bucket := cfg.Bucket(baseMs)
	dueMs := cfg.BucketStartMs(bucket) + cfg.TimeBucket.Milliseconds() + cfg.TimeBucketGrace.Milliseconds()

	sealed, err := store.SealDue(ctx, dueMs)
	require.NoError(t, err)
	require.Equal(t, 1, sealed)
	files, err := store.LocalParquet(key)
	require.NoError(t, err)
	require.Len(t, files, 1)
	require.Nil(t, files[0].UploadedAtMs, "the seal is pending upload")
	lostPath := files[0].Path

	// kill -9 between the seal commit and the upload, and the pending file is
	// gone (the loss window).
	require.NoError(t, os.Remove(lostPath))
	require.NoError(t, store.Close())

	store, err = Open(Config{DataDir: dataDir})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()
	require.NoError(t, store.Recover(ctx))

	files, err = store.LocalParquet(key)
	require.NoError(t, err)
	assert.Empty(t, files, "the lost pending row is dropped")
	unsealed, err := store.db.HasUnsealedCalls(key.String())
	require.NoError(t, err)
	assert.True(t, unsealed, "the lost parquet's calls are unsealed again (false on the pre-fix drop-only path)")

	// The seal loop rebuilds a discoverable replacement over the same rows.
	sealed, err = store.SealDue(ctx, dueMs)
	require.NoError(t, err)
	assert.Equal(t, 1, sealed, "the bucket re-seals the recovered calls")
	files, err = store.LocalParquet(key)
	require.NoError(t, err)
	require.Len(t, files, 1)
	assert.Equal(t, 2, files[0].RowCount, "both calls are back in a pending parquet")
	assert.Nil(t, files[0].UploadedAtMs)
}

// TestRecoverReSealsLostSecondPassParquet pins that the rewind is scoped to the
// LOST pass, not the whole bucket. With two seal passes, losing the second
// pass's pending file rewinds only to that pass's start (wal_offset_lo > 0), so
// the first pass's already-sealed rows stay below the watermark and are not
// re-exposed. It guards the pass-start stamping across multiple passes: a lo
// stamped as 0 would re-seal the first pass too (RowCount 2, not 1), and a lo
// stamped as the pass-end watermark would under-expose the lost call.
func TestRecoverReSealsLostSecondPassParquet(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	store, err := Open(Config{DataDir: dataDir})
	require.NoError(t, err)

	key := PodRestartKey{Namespace: "ns", Service: "svc", PodName: "pod", RestartTimeMs: 1_000}
	pr, err := store.OpenPodRestart(key)
	require.NoError(t, err)

	baseMs := int64(1_700_000_000_000)
	call := func(tsMs int64, off int) {
		require.NoError(t, pr.AppendCall(tsMs, data.Call{
			Method: 0, Duration: 10, ThreadName: "main",
			TraceFileIndex: 1, BufferOffset: off, RecordIndex: 0,
		}))
	}

	cfg := store.Config()
	bucket := cfg.Bucket(baseMs)
	dueMs := cfg.BucketStartMs(bucket) + cfg.TimeBucket.Milliseconds() + cfg.TimeBucketGrace.Milliseconds()

	// Pass 1 seals two calls from watermark 0.
	call(baseMs, 0)
	call(baseMs+1, 100)
	sealed, err := store.SealDue(ctx, dueMs)
	require.NoError(t, err)
	require.Equal(t, 1, sealed)

	// The watermark after pass 1 is exactly pass 2's wal_offset_lo, and it is
	// past 0 — so a correct rewind here must NOT reach the first-pass calls.
	pass2Lo, err := store.db.SealWatermark(key.String(), bucket)
	require.NoError(t, err)
	require.Greater(t, pass2Lo, int64(0), "pass 2 starts past the first-pass calls, not at 0")

	// Pass 2 patch-seals one late call from pass2Lo.
	call(baseMs+2, 200)
	sealed, err = store.SealDue(ctx, dueMs)
	require.NoError(t, err)
	require.Equal(t, 1, sealed)

	files, err := store.LocalParquet(key)
	require.NoError(t, err)
	require.Len(t, files, 2)
	var pass1Path, pass2Path string
	for _, f := range files {
		switch f.RowCount {
		case 2:
			pass1Path = f.Path
		case 1:
			pass2Path = f.Path
		}
	}
	require.NotEmpty(t, pass1Path, "the two-row first-pass file")
	require.NotEmpty(t, pass2Path, "the one-row patch file")

	// The second pass's pending file is lost before upload; the first pass's
	// survives on disk.
	require.NoError(t, os.Remove(pass2Path))
	require.NoError(t, store.Close())

	store, err = Open(Config{DataDir: dataDir})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()
	require.NoError(t, store.Recover(ctx))

	// Rewound to pass 2's start, NOT to 0.
	wm, err := store.db.SealWatermark(key.String(), bucket)
	require.NoError(t, err)
	assert.Equal(t, pass2Lo, wm, "the rewind targets the lost pass's start, not the whole bucket")

	files, err = store.LocalParquet(key)
	require.NoError(t, err)
	require.Len(t, files, 1, "only the lost pending row is dropped; the first pass survives")
	assert.Equal(t, pass1Path, files[0].Path)
	assert.Equal(t, 2, files[0].RowCount, "the surviving first-pass file is untouched")

	// The reseal rebuilds only the lost pass's single row — the first pass was
	// not re-exposed.
	sealed, err = store.SealDue(ctx, dueMs)
	require.NoError(t, err)
	assert.Equal(t, 1, sealed)
	files, err = store.LocalParquet(key)
	require.NoError(t, err)
	require.Len(t, files, 2)
	var reseal *ParquetLocalFile
	for i := range files {
		if files[i].Path != pass1Path {
			reseal = &files[i]
		}
	}
	require.NotNil(t, reseal, "a replacement for the lost pass is sealed")
	assert.Equal(t, 1, reseal.RowCount, "only the second pass's call re-seals; the first pass was not re-exposed")
}

// TestRecoverDropsUploadedMissingParquet pins the benign side of the QA 708#2
// fix: a file already durable in S3 (uploaded_at set) whose local copy the
// hot-retention janitor removed — crashing before it deleted the row — is
// dropped WITHOUT a re-seal. Forcing a reseal here would waste work re-uploading
// data the cold tier already holds.
func TestRecoverDropsUploadedMissingParquet(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	store, err := Open(Config{DataDir: dataDir})
	require.NoError(t, err)

	key := PodRestartKey{Namespace: "ns", Service: "svc", PodName: "pod", RestartTimeMs: 1_000}
	pr, err := store.OpenPodRestart(key)
	require.NoError(t, err)
	baseMs := int64(1_700_000_000_000)
	require.NoError(t, pr.AppendCall(baseMs, data.Call{
		Method: 0, Duration: 10, ThreadName: "main",
		TraceFileIndex: 1, BufferOffset: 0, RecordIndex: 0,
	}))

	cfg := store.Config()
	bucket := cfg.Bucket(baseMs)
	dueMs := cfg.BucketStartMs(bucket) + cfg.TimeBucket.Milliseconds() + cfg.TimeBucketGrace.Milliseconds()
	sealed, err := store.SealDue(ctx, dueMs)
	require.NoError(t, err)
	require.Equal(t, 1, sealed)
	files, err := store.LocalParquet(key)
	require.NoError(t, err)
	require.Len(t, files, 1)
	uploadedPath := files[0].Path

	// The PUT confirmed (uploaded_at set, refs released); later the hot-retention
	// janitor removed the local copy but crashed before deleting the row.
	require.NoError(t, store.db.MarkUploaded(uploadedPath, time.Now().UnixMilli()))
	require.NoError(t, os.Remove(uploadedPath))
	require.NoError(t, store.Close())

	store, err = Open(Config{DataDir: dataDir})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()
	require.NoError(t, store.Recover(ctx))

	files, err = store.LocalParquet(key)
	require.NoError(t, err)
	assert.Empty(t, files, "the uploaded-but-locally-gone row is dropped")
	unsealed, err := store.db.HasUnsealedCalls(key.String())
	require.NoError(t, err)
	assert.False(t, unsealed, "durable-in-S3 data is not needlessly re-sealed")
	sealed, err = store.SealDue(ctx, dueMs)
	require.NoError(t, err)
	assert.Zero(t, sealed, "no re-seal for data already in the cold tier")
}
