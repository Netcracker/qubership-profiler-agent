package hotstore

// Regression for QA finding 708#8 / reports2#4: a permanently rejected pods/v1
// identity manifest must never leave a discoverable-but-nameless completed
// bundle. The manifest is the only cold source of the pod-restart's readable
// identity behind the one-way hash in the parquet key (01 §3.6, 02 §2.7), so
// the parquet's completion is coupled to the manifest — rejected means pending,
// and the №2 slow re-test recovers both once S3 takes the manifest.

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// manifestRejectingStore is an in-test S3 that accepts every parquet PUT but
// can permanently reject the pods/v1 manifest — exactly the reports2#4 fault:
// only the identity manifest upload is refused.
type manifestRejectingStore struct {
	rejectManifest atomic.Bool
	parquetPuts    atomic.Int64
	manifestPuts   atomic.Int64
}

func (s *manifestRejectingStore) PutFile(context.Context, string, string) error {
	s.parquetPuts.Add(1)
	return nil
}

func (s *manifestRejectingStore) PutBytes(_ context.Context, key string, _ []byte) error {
	if strings.HasPrefix(key, "pods/v1/") {
		s.manifestPuts.Add(1)
		if s.rejectManifest.Load() {
			return &PermanentUploadError{Err: errors.New("403 forbidden manifest")}
		}
	}
	return nil
}

// TestRejectedManifestKeepsPodPendingAndRecovers pins the coupling: with the
// pods/v1 manifest permanently rejected, the parquet is NOT marked complete —
// it is quarantined so the pod-restart stays discoverable/pending across hot
// retention — and once the rejection heals the №2 slow re-test writes the
// manifest and completes the bundle. Reverting upsertManifest to mark the
// parquet uploaded on a rejected manifest fails at the uploaded_at / pending
// assertions after pass 1.
func TestRejectedManifestKeepsPodPendingAndRecovers(t *testing.T) {
	ctx := context.Background()
	store, err := Open(Config{
		DataDir:                  t.TempDir(),
		UploadRetryAttempts:      1,
		UploadRetryBaseDelay:     time.Millisecond,
		QuarantineRetestInterval: time.Millisecond,
		HotRetention:             time.Minute,
	})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	s3 := &manifestRejectingStore{}
	s3.rejectManifest.Store(true)
	uploader := NewUploader(store, s3)

	key := PodRestartKey{Namespace: "ns", Service: "svc", PodName: "pod-m", RestartTimeMs: janitorCallTs}
	require.NoError(t, store.db.UpsertPodRestart(key, janitorCallTs))
	require.NoError(t, store.db.ClosePodRestart(key, janitorCallTs))

	bucket := store.cfg.Bucket(janitorCallTs)
	bucketStart := store.cfg.BucketStartMs(bucket)
	// One sealed file plus its indexed call, so the pod-restart is discoverable
	// in the hot tier the way cold /pods will later need the manifest to be.
	sealedPath := filepath.Join(store.cfg.DataDir, "sealed-m.parquet")
	require.NoError(t, os.WriteFile(sealedPath, []byte("parquet"), 0o644))
	require.NoError(t, store.db.RecordSealedFile(parquetLocalRow{
		Path: sealedPath, PodRestart: key.String(), TimeBucketMs: bucketStart,
		RetentionClass: RetentionShortClean, Seq: 0, RowCount: 1,
		TimeMinMs: janitorCallTs, TimeMaxMs: janitorCallTs + 1, FileSize: 7, SealedAtMs: janitorCallTs,
		S3Key: "parquet/v1/short_clean/x/sealed-m.parquet",
	}, nil))
	require.NoError(t, store.db.InsertCall(bucket, CallIndexRow{
		PodRestart: key.String(), TraceFileIndex: 1, BufferOffset: 0, RecordIndex: 0,
		TsMs: janitorCallTs, RetentionClass: RetentionShortClean, CallsWalOffset: 0,
	}))

	discoverable := func() bool {
		windows, err := store.PodWindows()
		require.NoError(t, err)
		_, ok := windows[key.String()]
		return ok
	}
	require.True(t, discoverable(), "the pod-restart is discoverable in the hot tier before upload")

	// Pass 1: the parquet uploads, but the manifest is permanently rejected.
	stats, err := uploader.Pass(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 1, s3.parquetPuts.Load(), "the parquet PUT still runs")
	assert.GreaterOrEqual(t, s3.manifestPuts.Load(), int64(1), "the manifest PUT was attempted")
	assert.Zero(t, stats.UploadedFiles,
		"the bundle must NOT be marked complete while its identity manifest is missing")
	assert.EqualValues(t, 1, stats.QuarantinedObjects, "the rejected manifest body is quarantined for a human")
	assert.EqualValues(t, 1, stats.QuarantinedFiles, "the parquet is quarantined alongside it, not marked uploaded")

	// The parquet_local row stays pending (uploaded_at NULL): the pod-restart is
	// visibly pending, the WAL purge is blocked, and hot discovery still sees it.
	files, err := store.db.LocalParquet(key.String())
	require.NoError(t, err)
	require.Len(t, files, 1)
	assert.Nil(t, files[0].UploadedAtMs, "uploaded_at must stay NULL until the manifest is durable")
	require.NotNil(t, files[0].UploadFailedAtMs, "the bundle is visibly quarantined, not silently done")
	assert.Equal(t, sealedPath, files[0].Path, "the durable parquet is not moved; only its queue state changes")
	pending, err := store.db.HasPendingParquet(key.String())
	require.NoError(t, err)
	assert.True(t, pending, "a pending manifest blocks the WAL purge, so the identity is never dropped")
	q, err := store.QuarantineStats()
	require.NoError(t, err)
	assert.EqualValues(t, 1, q.ParquetCount)

	// Advance past the WAL-purge grace: the janitor must NOT age the bundle out
	// (uploaded_at is NULL) or purge the WALs, so the pod-restart never becomes a
	// discoverable-but-identityless completed call. It stays discoverable.
	nowMs := janitorCallTs + 2*time.Hour.Milliseconds()
	jstats, err := store.JanitorPass(ctx, nowMs)
	require.NoError(t, err)
	assert.Zero(t, jstats.ParquetDeleted, "a pending bundle is never aged out")
	assert.Zero(t, jstats.PartitionsDropped, "its bucket partition stays while the manifest is missing")
	assert.Zero(t, jstats.WalsPurged, "the WAL purge waits on the pending manifest")
	assert.Zero(t, jstats.QuarantineDropped, "the bundle is inside the quarantine cap")
	assert.True(t, discoverable(), "the pod-restart stays discoverable in the hot tier")

	// The rejection heals (a bucket-policy or credential fix); the retest interval
	// elapses and the next pass re-queues, re-PUTs, and the manifest sticks.
	s3.rejectManifest.Store(false)
	time.Sleep(5 * time.Millisecond)
	stats, err = uploader.Pass(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 1, stats.RequeuedFiles, "the slow re-test re-queues the quarantined bundle")
	assert.EqualValues(t, 1, stats.UploadedFiles, "once the manifest is durable the bundle completes")
	assert.EqualValues(t, 1, stats.ManifestPuts, "the manifest is written on the recovery pass")

	files, err = store.db.LocalParquet(key.String())
	require.NoError(t, err)
	require.Len(t, files, 1)
	require.NotNil(t, files[0].UploadedAtMs, "uploaded_at is set only after the manifest is durable")
	q, err = store.QuarantineStats()
	require.NoError(t, err)
	assert.Zero(t, q.ParquetCount, "recovery empties the quarantine")
	backlog, _, err := store.UploadBacklog()
	require.NoError(t, err)
	assert.Zero(t, backlog)

	// The stale rejected manifest body is cleared once the real one is durable.
	var leftover []string
	_ = filepath.WalkDir(filepath.Join(store.cfg.DataDir, "upload-failed"),
		func(p string, d os.DirEntry, err error) error {
			if err == nil && !d.IsDir() {
				leftover = append(leftover, p)
			}
			return nil
		})
	assert.Empty(t, leftover, "upload-failed/ keeps no stale copy of a now-durable manifest")
}

// TestStuckManifestAgesOutOnFirstFailure pins the give-up path: a manifest that
// never heals keeps an immutable first_failed_at across every re-test cycle
// (upload_failed_at advances, first_failed_at does not), the stuck-quarantine
// gauge climbs from the first failure, and the quarantine age cap eventually
// drops the bundle. Driving the age arm off the re-test-reset upload_failed_at
// (the old behavior) would never fire, so this pins the reset-defeats-age fix.
func TestStuckManifestAgesOutOnFirstFailure(t *testing.T) {
	ctx := context.Background()
	store, err := Open(Config{
		DataDir:                  t.TempDir(),
		UploadRetryAttempts:      1,
		UploadRetryBaseDelay:     time.Millisecond,
		QuarantineRetestInterval: time.Millisecond,
		QuarantineMaxAge:         time.Hour,
		QuarantineMaxBytes:       1 << 30,
	})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	s3 := &manifestRejectingStore{}
	s3.rejectManifest.Store(true) // the rejection never heals
	uploader := NewUploader(store, s3)

	key := PodRestartKey{Namespace: "ns", Service: "svc", PodName: "pod-stuck", RestartTimeMs: janitorCallTs}
	require.NoError(t, store.db.UpsertPodRestart(key, janitorCallTs))
	bucket := store.cfg.Bucket(janitorCallTs)
	bucketStart := store.cfg.BucketStartMs(bucket)
	sealedPath := filepath.Join(store.cfg.DataDir, "sealed-stuck.parquet")
	require.NoError(t, os.WriteFile(sealedPath, []byte("parquet"), 0o644))
	require.NoError(t, store.db.RecordSealedFile(parquetLocalRow{
		Path: sealedPath, PodRestart: key.String(), TimeBucketMs: bucketStart,
		RetentionClass: RetentionShortClean, Seq: 0, RowCount: 1,
		TimeMinMs: janitorCallTs, TimeMaxMs: janitorCallTs + 1, FileSize: 7, SealedAtMs: janitorCallTs,
		S3Key: "parquet/v1/short_clean/x/sealed-stuck.parquet",
	}, nil))

	failTimes := func() (first, last int64) {
		files, err := store.db.LocalParquet(key.String())
		require.NoError(t, err)
		require.Len(t, files, 1)
		require.NotNil(t, files[0].FirstFailedAtMs, "the first quarantine stamps first_failed_at")
		require.NotNil(t, files[0].UploadFailedAtMs)
		return *files[0].FirstFailedAtMs, *files[0].UploadFailedAtMs
	}

	// First rejection quarantines and stamps first_failed_at.
	_, err = uploader.Pass(ctx)
	require.NoError(t, err)
	ff0, uf := failTimes()

	// Two more re-test cycles: upload_failed_at advances, first_failed_at is frozen.
	for i := 0; i < 2; i++ {
		time.Sleep(2 * time.Millisecond)
		_, err = uploader.Pass(ctx)
		require.NoError(t, err)
		ff, ufNow := failTimes()
		assert.Equal(t, ff0, ff, "first_failed_at is immutable across re-test cycles")
		assert.Greater(t, ufNow, uf, "upload_failed_at advances on each re-test")
		uf = ufNow
	}

	// The stuck-quarantine gauge reads the frozen first failure, not the reset.
	q, err := store.QuarantineStats()
	require.NoError(t, err)
	require.NotNil(t, q.ParquetOldestMs)
	assert.Equal(t, ff0, *q.ParquetOldestMs, "the age gauge climbs from the first failure, not the last re-test")

	// At first_failed_at + QuarantineMaxAge the age arm fires. Keying it off the
	// (later) upload_failed_at would leave the bundle pinned here — the fix.
	require.Greater(t, uf, ff0, "the re-tests moved upload_failed_at past first_failed_at")
	nowMs := ff0 + store.cfg.QuarantineMaxAge.Milliseconds()
	stats, err := store.JanitorPass(ctx, nowMs)
	require.NoError(t, err)
	assert.EqualValues(t, 1, stats.QuarantineDropped,
		"a manifest stuck past the age cap is dropped by the first-failure age arm")
	assert.NoFileExists(t, sealedPath)
	q, err = store.QuarantineStats()
	require.NoError(t, err)
	assert.Zero(t, q.ParquetCount, "the aged-out bundle leaves the quarantine")
}
