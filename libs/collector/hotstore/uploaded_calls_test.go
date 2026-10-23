package hotstore

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestUploadedCalls pins the per-pass upload state /internal/v1/stats relies
// on (02 §2.9): a call counts as uploaded only when it is below the seal
// watermark and every file of the seal pass that covers its calls.wal offset
// is confirmed in S3.
func TestUploadedCalls(t *testing.T) {
	store, err := Open(Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()
	const pr = "ns:svc:pod-a:1"
	bucket := int64(10)
	bucketMs := store.cfg.BucketStartMs(bucket)
	seq := 0
	// seal records one pass: a file per class, all starting at lo, and the
	// watermark advanced to hi.
	seal := func(lo, hi int64, classes ...string) []string {
		var commits []sealCommit
		var paths []string
		for _, class := range classes {
			path := fmt.Sprintf("%s/f-%d.parquet", t.TempDir(), seq)
			seq++
			paths = append(paths, path)
			commits = append(commits, sealCommit{row: parquetLocalRow{Path: path, PodRestart: pr,
				TimeBucketMs: bucketMs, RetentionClass: class, Seq: seq, WalOffsetLo: lo, S3Key: path}})
		}
		require.NoError(t, store.db.CommitSealPass(pr, bucket, commits, hi, 1))
		return paths
	}
	uploaded := func(off int64) bool {
		t.Helper()
		u, err := store.UploadedCalls()
		require.NoError(t, err)
		return u.Uploaded(bucket, pr, off)
	}

	assert.False(t, uploaded(0), "nothing sealed")
	first := seal(0, 100, RetentionShortClean, RetentionAnyError)
	second := seal(100, 200, RetentionShortClean)
	assert.False(t, uploaded(50), "sealed, not uploaded")

	require.NoError(t, store.db.MarkUploaded(first[0], 2))
	assert.False(t, uploaded(50), "one file of the pass is still pending")
	require.NoError(t, store.db.MarkUploaded(first[1], 2))
	assert.True(t, uploaded(0))
	assert.True(t, uploaded(99))
	assert.False(t, uploaded(100), "the second pass is pending")
	assert.False(t, uploaded(200), "at the watermark: not sealed yet")

	require.NoError(t, store.db.MarkUploadFailed(second[0], second[0]+".failed", 3))
	assert.False(t, uploaded(150), "a quarantined file is not in S3")

	t.Run("other pod-restarts and buckets know nothing", func(t *testing.T) {
		u, err := store.UploadedCalls()
		require.NoError(t, err)
		assert.False(t, u.Uploaded(bucket, "ns:svc:pod-b:1", 50))
		assert.False(t, u.Uploaded(bucket+1, pr, 50))
	})
	t.Run("the janitor forgetting aged files keeps them uploaded", func(t *testing.T) {
		for _, path := range first {
			require.NoError(t, store.db.DeleteParquetLocalRow(path))
		}
		assert.True(t, uploaded(50), "only uploaded files age out")
	})
	t.Run("a rewound watermark re-exposes the calls", func(t *testing.T) {
		require.NoError(t, store.db.RecoverLostPendingParquet(second[0]+".failed", pr, bucket, 100))
		assert.True(t, uploaded(99))
		assert.False(t, uploaded(150))
	})
}
