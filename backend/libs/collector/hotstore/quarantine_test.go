package hotstore

// Regression for QA finding 708#3: the genuine parquet quarantine (an S3 PUT a
// retry cannot fix moves the file to upload-failed/). Two coupled defects:
//
//  1. The destination was filepath.Base(f.Path) into a single flat
//     upload-failed/ directory. The sealed basename omits the retention class —
//     it lives only in the S3-key directory — so two files of different classes
//     that share those name fields collide and overwrite each other (data loss).
//  2. MarkUploadFailed moved parquet_local.path to the quarantine path but left
//     the parquet_segments refs on the old path, so releasing them later (on
//     re-upload or drop, both keyed on the current path) matched nothing and the
//     segment refcounts stayed pinned forever (unbounded local-storage growth).
//
// The two tests below pin both fixes; each fails if its fix is reverted.

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestQuarantineOfSameBasenameDifferentClassKeepsBothFiles pins defect 1: two
// sealed files of different retention classes that share a basename are both
// quarantined to distinct paths under upload-failed/, so neither overwrites the
// other. A flat filepath.Base destination (the pre-fix behavior) would rename
// the second file over the first, and only one body would survive.
func TestQuarantineOfSameBasenameDifferentClassKeepsBothFiles(t *testing.T) {
	ctx := context.Background()
	store, err := Open(Config{
		DataDir:              t.TempDir(),
		UploadRetryAttempts:  1,
		UploadRetryBaseDelay: time.Millisecond,
	})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	key := PodRestartKey{Namespace: "ns", Service: "svc", PodName: "pod-q", RestartTimeMs: janitorCallTs}
	require.NoError(t, store.db.UpsertPodRestart(key, janitorCallTs))

	// The class appears only in the S3-key directory, never in the basename, so
	// these two files collide by construction on a flat quarantine scheme.
	base := "repl-hash-100-200-300-0.parquet"
	seed := func(class, body string) string {
		s3Key := "parquet/v1/" + class + "/2024/01/01/00/" + base
		path := filepath.Join(store.cfg.DataDir, filepath.FromSlash(s3Key))
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, []byte(body), 0o644))
		require.NoError(t, store.db.RecordSealedFile(parquetLocalRow{
			Path: path, PodRestart: key.String(), TimeBucketMs: 0,
			RetentionClass: class, Seq: 0, RowCount: 1,
			TimeMinMs: 1, TimeMaxMs: 2, FileSize: int64(len(body)), SealedAtMs: janitorCallTs,
			S3Key: s3Key,
		}, nil))
		return path
	}
	pathA := seed(RetentionShortClean, "aaa")
	pathB := seed(RetentionLongClean, "bbb")
	require.Equal(t, filepath.Base(pathA), filepath.Base(pathB),
		"the two files share a basename by construction")

	s3 := &fakeObjectStore{}
	s3.failPermanent.Store(true)
	uploader := NewUploader(store, s3)

	stats, err := uploader.Pass(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 2, stats.QuarantinedFiles, "both files are quarantined")

	files, err := store.db.LocalParquet(key.String())
	require.NoError(t, err)
	require.Len(t, files, 2)

	uploadFailedDir := filepath.Join(store.cfg.DataDir, "upload-failed")
	quarantinePaths := map[string]string{}
	bodies := map[string]string{}
	for _, f := range files {
		require.True(t, strings.HasPrefix(f.Path, uploadFailedDir),
			"a quarantined parquet lives under upload-failed/ for the drop-log heuristic")
		body, err := os.ReadFile(f.Path)
		require.NoError(t, err, "the quarantined file survives on disk")
		quarantinePaths[f.RetentionClass] = f.Path
		bodies[f.RetentionClass] = string(body)
	}

	// Both originals moved, and both bodies survive at their own quarantine paths.
	assert.NoFileExists(t, pathA)
	assert.NoFileExists(t, pathB)
	assert.NotEqual(t, quarantinePaths[RetentionShortClean], quarantinePaths[RetentionLongClean],
		"same-basename files of different classes quarantine to distinct paths")
	assert.Equal(t, "aaa", bodies[RetentionShortClean], "the short_clean body is intact")
	assert.Equal(t, "bbb", bodies[RetentionLongClean],
		"the long_clean body is intact, not overwritten by the same-basename short_clean file")
}

// seedQuarantinedFile records one sealed file that pins a segment with pinRows
// refs, then runs one upload pass against a permanently-rejecting S3 so the file
// is quarantined under upload-failed/. It returns the quarantine path, a
// refcount reader, and the rejecting S3 so a follow-up pass can heal it.
func seedQuarantinedFile(t *testing.T, store *Store, pinRows int) (quarantinePath string, refcount func() int, s3 *fakeObjectStore) {
	t.Helper()
	ctx := context.Background()

	key := PodRestartKey{Namespace: "ns", Service: "svc", PodName: "pod-r", RestartTimeMs: janitorCallTs}
	require.NoError(t, store.db.UpsertPodRestart(key, janitorCallTs))
	require.NoError(t, store.db.UpsertSegment(key.String(), StreamTrace, 1, "/x/trace/000001.gz", 1))

	s3Key := "parquet/v1/short_clean/2024/01/01/00/repl-hash-100-200-300-0.parquet"
	sealedPath := filepath.Join(store.cfg.DataDir, filepath.FromSlash(s3Key))
	require.NoError(t, os.MkdirAll(filepath.Dir(sealedPath), 0o755))
	require.NoError(t, os.WriteFile(sealedPath, []byte("parquet"), 0o644))
	require.NoError(t, store.db.RecordSealedFile(parquetLocalRow{
		Path: sealedPath, PodRestart: key.String(), TimeBucketMs: 0,
		RetentionClass: RetentionShortClean, Seq: 0, RowCount: 1,
		TimeMinMs: 1, TimeMaxMs: 2, FileSize: 7, SealedAtMs: janitorCallTs,
		S3Key: s3Key,
	}, map[segKey]int{{StreamTrace, 1}: pinRows}))

	refcount = func() int {
		rc, _, err := store.db.SegmentRefcount(key.String(), StreamTrace, 1)
		require.NoError(t, err)
		return rc
	}
	require.Equal(t, pinRows, refcount(), "the sealed file pins the segment")

	s3 = &fakeObjectStore{}
	s3.failPermanent.Store(true)
	uploader := NewUploader(store, s3)

	stats, err := uploader.Pass(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 1, stats.QuarantinedFiles)
	assert.Equal(t, pinRows, refcount(), "a quarantined file keeps its segments pinned")

	files, err := store.db.LocalParquet(key.String())
	require.NoError(t, err)
	require.Len(t, files, 1)
	quarantinePath = files[0].Path
	require.True(t, strings.HasPrefix(quarantinePath, filepath.Join(store.cfg.DataDir, "upload-failed")),
		"the file moved under upload-failed/")
	require.NotEqual(t, sealedPath, quarantinePath)
	return quarantinePath, refcount, s3
}

// TestQuarantinedParquetReleasesSegmentsOnReupload pins defect 2 via the
// requeue -> re-upload path: once the rejection heals, the slow re-test
// re-queues the quarantined file, the re-upload succeeds, and MarkUploaded
// releases ALL its segment refs. Before the fix the parquet_segments refs still
// keyed off the pre-quarantine path, so the release matched nothing and the
// refcount stayed pinned forever.
func TestQuarantinedParquetReleasesSegmentsOnReupload(t *testing.T) {
	ctx := context.Background()
	store, err := Open(Config{
		DataDir:                  t.TempDir(),
		UploadRetryAttempts:      1,
		UploadRetryBaseDelay:     time.Millisecond,
		QuarantineRetestInterval: time.Millisecond,
	})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	_, refcount, s3 := seedQuarantinedFile(t, store, 5)
	uploader := NewUploader(store, s3)

	s3.failPermanent.Store(false)
	time.Sleep(5 * time.Millisecond)
	stats, err := uploader.Pass(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 1, stats.RequeuedFiles, "the slow re-test re-queues the quarantined file")
	assert.EqualValues(t, 1, stats.UploadedFiles, "the re-upload succeeds once the rejection heals")
	assert.Equal(t, 0, refcount(),
		"the re-uploaded quarantined file releases all its segment refs — no permanent pin")
}

// TestQuarantinedParquetReleasesSegmentsOnDrop pins defect 2 via the
// quarantine-cap drop path: DropParquetLocal on the quarantine path releases the
// migrated segment refs. Before the fix the refs stayed keyed on the old path,
// so the refcount stayed pinned even after the file was dropped.
func TestQuarantinedParquetReleasesSegmentsOnDrop(t *testing.T) {
	store, err := Open(Config{
		DataDir:              t.TempDir(),
		UploadRetryAttempts:  1,
		UploadRetryBaseDelay: time.Millisecond,
	})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	quarantinePath, refcount, _ := seedQuarantinedFile(t, store, 5)

	require.NoError(t, store.db.DropParquetLocal(quarantinePath))
	assert.Equal(t, 0, refcount(),
		"dropping a quarantined file releases all its segment refs — no permanent pin")
}
