package hotstore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMarkUploadedReleasesExactlyOnce pins invariant C1 at the SQLite layer:
// the refcount release rides the same transaction as uploaded_at, and the
// guard — deleting the file's parquet_segments rows in that transaction —
// makes a repeated MarkUploaded (recovery re-running the §6.2 step after a
// crash) decrement nothing, so a segment other files still pin survives.
func TestMarkUploadedReleasesExactlyOnce(t *testing.T) {
	store, err := Open(Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	key := PodRestartKey{Namespace: "ns", Service: "svc", PodName: "pod", RestartTimeMs: 42}
	require.NoError(t, store.db.UpsertPodRestart(key, 1))
	require.NoError(t, store.db.UpsertSegment(key.String(), StreamTrace, 1, "/x/trace/000001.gz", 1))

	sealedRow := func(path string, seq int) parquetLocalRow {
		return parquetLocalRow{
			Path: path, PodRestart: key.String(), TimeBucketMs: 0,
			RetentionClass: RetentionShortClean, Seq: seq, RowCount: 1,
			TimeMinMs: 1, TimeMaxMs: 2, FileSize: 10, SealedAtMs: 3,
			S3Key: "parquet/v1/short_clean/x-" + path,
		}
	}
	// Two sealed files pin the same segment: 3 rows + 2 rows.
	require.NoError(t, store.db.RecordSealedFile(sealedRow("/x/a.parquet", 0),
		map[segKey]int{{StreamTrace, 1}: 3}))
	require.NoError(t, store.db.RecordSealedFile(sealedRow("/x/b.parquet", 1),
		map[segKey]int{{StreamTrace, 1}: 2}))

	refcount := func() int {
		segments, err := store.db.Segments(key.String())
		require.NoError(t, err)
		require.Len(t, segments, 1)
		return segments[0].Refcount
	}
	require.Equal(t, 5, refcount())

	require.NoError(t, store.db.MarkUploaded("/x/a.parquet", 100))
	assert.Equal(t, 2, refcount(), "the first release decrements exactly the rows file a pinned")

	require.NoError(t, store.db.MarkUploaded("/x/a.parquet", 200))
	assert.Equal(t, 2, refcount(), "a repeated release must be a no-op, not another decrement")

	files, err := store.db.LocalParquet(key.String())
	require.NoError(t, err)
	require.Len(t, files, 2)
	require.NotNil(t, files[0].UploadedAtMs)
	assert.Equal(t, int64(100), *files[0].UploadedAtMs, "the repeat must not restamp uploaded_at")
	assert.Nil(t, files[1].UploadedAtMs)

	require.NoError(t, store.db.MarkUploaded("/x/b.parquet", 300))
	assert.Equal(t, 0, refcount(), "file b's release brings the segment to zero")
}
