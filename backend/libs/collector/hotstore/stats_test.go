package hotstore

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestJanitorCountersAccumulate pins the process-lifetime accumulator: the
// snapshot must equal the sum of every pass's stats, including passes that did
// nothing — the Prometheus counters read the snapshot, not the last pass.
func TestJanitorCountersAccumulate(t *testing.T) {
	ctx := context.Background()
	store, err := Open(Config{DataDir: t.TempDir(), ChunksStagingMaxBytes: 150})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	key := PodRestartKey{Namespace: "ns", Service: "svc", PodName: "pod-c", RestartTimeMs: janitorCallTs}
	require.NoError(t, store.db.UpsertPodRestart(key, janitorCallTs))
	for seq := 1; seq <= 3; seq++ {
		path := filepath.Join(store.cfg.DataDir, fmt.Sprintf("seg-%d.gz", seq))
		require.NoError(t, os.WriteFile(path, bytes.Repeat([]byte{0xCD}, 100), 0o644))
		require.NoError(t, store.db.UpsertSegment(key.String(), StreamTrace, seq, path, int64(seq)))
		require.NoError(t, store.db.FinalizeSegment(key.String(), StreamTrace, seq, 100, nil, nil))
	}

	stats1, err := store.JanitorPass(ctx, janitorCallTs)
	require.NoError(t, err)
	require.EqualValues(t, 2, stats1.SegmentsEvicted, "300 bytes over a 150 budget evicts two segments")

	stats2, err := store.JanitorPass(ctx, janitorCallTs)
	require.NoError(t, err)
	assert.Zero(t, stats2.SegmentsEvicted)

	snap := store.JanitorCountersSnapshot()
	assert.EqualValues(t, 2, snap.SegmentsEvicted)
	assert.EqualValues(t, 200, snap.EvictedBytes)

	bytesOnDisk, budget := store.SegmentsDiskUsage()
	assert.EqualValues(t, 100, bytesOnDisk, "the gauge reflects the post-eviction total")
	assert.EqualValues(t, 150, budget)
}

// TestEvictedChunkRefsGauge pins the risk-B-3 gauge: in-RAM chunk refs whose
// trace segment was evicted are counted by the janitor pass, and refs into
// live segments are not.
func TestEvictedChunkRefsGauge(t *testing.T) {
	ctx := context.Background()
	store, err := Open(Config{DataDir: t.TempDir(), ChunksStagingMaxBytes: 150})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	key := PodRestartKey{Namespace: "ns", Service: "svc", PodName: "pod-b3", RestartTimeMs: janitorCallTs}
	pr, err := store.OpenPodRestart(key)
	require.NoError(t, err)

	for seq := 1; seq <= 2; seq++ {
		path := filepath.Join(store.cfg.DataDir, fmt.Sprintf("b3-seg-%d.gz", seq))
		require.NoError(t, os.WriteFile(path, bytes.Repeat([]byte{0xEF}, 100), 0o644))
		require.NoError(t, store.db.UpsertSegment(key.String(), StreamTrace, seq, path, int64(seq)))
		require.NoError(t, store.db.FinalizeSegment(key.String(), StreamTrace, seq, 100, nil, nil))
	}
	// Recovery-style direct index fill: three refs into segment 1 (the oldest,
	// evicted first) and one into segment 2 (survives under the budget).
	pr.mu.Lock()
	pr.chunks[7] = []ChunkRef{
		{RollingSeq: 1, Offset: 0, Length: 10},
		{RollingSeq: 1, Offset: 10, Length: 10},
		{RollingSeq: 2, Offset: 0, Length: 10},
	}
	pr.chunks[9] = []ChunkRef{{RollingSeq: 1, Offset: 20, Length: 10}}
	pr.mu.Unlock()

	stats, err := store.JanitorPass(ctx, janitorCallTs)
	require.NoError(t, err)
	require.EqualValues(t, 1, stats.SegmentsEvicted, "200 bytes over a 150 budget evicts the oldest segment")
	assert.EqualValues(t, 3, store.EvictedChunkRefs(),
		"both threads' refs into the evicted segment count; the live segment's ref does not")
}

// TestQuarantineStats pins the stuck-quarantine gauges: the count and the
// oldest failure time of quarantined parquet, empty when nothing is stuck.
func TestQuarantineStats(t *testing.T) {
	store, err := Open(Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	empty, err := store.QuarantineStats()
	require.NoError(t, err)
	assert.Zero(t, empty.ParquetCount)
	assert.Nil(t, empty.ParquetOldestMs)

	key := PodRestartKey{Namespace: "ns", Service: "svc", PodName: "pod-q", RestartTimeMs: janitorCallTs}
	require.NoError(t, store.db.UpsertPodRestart(key, janitorCallTs))
	bucket := store.cfg.Bucket(janitorCallTs)
	older := seedSealedFile(t, store, key, bucket, 0)
	newer := seedSealedFile(t, store, key, bucket, 1)
	require.NoError(t, store.db.MarkUploadFailed(older, older+".failed", janitorCallTs+minute))
	require.NoError(t, store.db.MarkUploadFailed(newer, newer+".failed", janitorCallTs+2*minute))

	stats, err := store.QuarantineStats()
	require.NoError(t, err)
	assert.EqualValues(t, 2, stats.ParquetCount)
	require.NotNil(t, stats.ParquetOldestMs)
	assert.Equal(t, janitorCallTs+minute, *stats.ParquetOldestMs, "oldest failure wins")
}

// TestPutWithRetryCountsFailures pins the PUT counters: every call counts as
// an attempt of its object kind, every failed call as a failure with its
// reason, and RetriedPuts counts only the failed attempts a retry followed.
func TestPutWithRetryCountsFailures(t *testing.T) {
	store, err := Open(Config{DataDir: t.TempDir(), UploadRetryBaseDelay: time.Millisecond})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	t.Run("transient failures then success", func(t *testing.T) {
		u := NewUploader(store, nil)
		var stats UploadStats
		calls := 0
		err := u.putWithRetry(context.Background(), PutObjectParquet, "k", func() error {
			calls++
			if calls <= 2 {
				return errors.New("503")
			}
			return nil
		}, &stats)
		require.NoError(t, err)
		assert.EqualValues(t, 3, u.PutAttempts(PutObjectParquet), "PutAttempts(parquet)")
		assert.EqualValues(t, 2, u.PutFailures(PutObjectParquet, PutFailureTransient), "PutFailures(parquet, transient)")
		assert.Zero(t, u.PutFailures(PutObjectParquet, PutFailurePermanent), "PutFailures(parquet, permanent)")
		assert.Zero(t, u.PutAttempts(PutObjectManifest), "PutAttempts(manifest)")
		assert.EqualValues(t, 2, stats.RetriedPuts, "RetriedPuts")
	})

	t.Run("permanent rejection", func(t *testing.T) {
		u := NewUploader(store, nil)
		var stats UploadStats
		err := u.putWithRetry(context.Background(), PutObjectManifest, "k", func() error {
			return &PermanentUploadError{Err: errors.New("403")}
		}, &stats)
		require.Error(t, err)
		assert.EqualValues(t, 1, u.PutAttempts(PutObjectManifest), "PutAttempts(manifest)")
		assert.EqualValues(t, 1, u.PutFailures(PutObjectManifest, PutFailurePermanent), "PutFailures(manifest, permanent)")
		assert.Zero(t, u.PutFailures(PutObjectManifest, PutFailureTransient), "PutFailures(manifest, transient)")
		assert.Zero(t, u.PutAttempts(PutObjectParquet), "PutAttempts(parquet)")
		assert.Zero(t, stats.RetriedPuts, "no retry follows a permanent rejection")
	})
}

// blockingObjectStore fails the first parquet PUT with a transient error and
// blocks the second until release is closed, signalling on blocked first.
type blockingObjectStore struct {
	calls   atomic.Int64
	blocked chan struct{}
	release chan struct{}
}

func (s *blockingObjectStore) PutFile(context.Context, string, string) error {
	switch s.calls.Add(1) {
	case 1:
		return errors.New("503 slow down")
	case 2:
		close(s.blocked)
		<-s.release
	}
	return nil
}

func (s *blockingObjectStore) PutBytes(context.Context, string, []byte) error { return nil }

// TestPutCountersAreLiveDuringPass pins that the PUT counters move while a
// pass is still running: an alert must see an S3 outage inside one long pass,
// and CountersSnapshot moves only when the pass ends.
func TestPutCountersAreLiveDuringPass(t *testing.T) {
	ctx := context.Background()
	store, err := Open(Config{DataDir: t.TempDir(), UploadRetryBaseDelay: time.Millisecond})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()
	key := PodRestartKey{Namespace: "ns", Service: "svc", PodName: "pod-live", RestartTimeMs: janitorCallTs}
	require.NoError(t, store.db.UpsertPodRestart(key, janitorCallTs))
	seedPendingParquet(t, store, key, 0, 7)

	s3 := &blockingObjectStore{blocked: make(chan struct{}), release: make(chan struct{})}
	u := NewUploader(store, s3)
	passErr := make(chan error, 1)
	go func() {
		_, err := u.Pass(ctx)
		passErr <- err
	}()

	select {
	case <-s3.blocked:
	case <-time.After(10 * time.Second):
		close(s3.release)
		t.Fatal("the second parquet PUT never started")
	}
	assert.EqualValues(t, 2, u.PutAttempts(PutObjectParquet), "PutAttempts(parquet) while the second PUT blocks")
	assert.EqualValues(t, 1, u.PutFailures(PutObjectParquet, PutFailureTransient),
		"PutFailures(parquet, transient) while the second PUT blocks")
	assert.Equal(t, UploadStats{}, u.CountersSnapshot(), "CountersSnapshot before the pass ends")

	close(s3.release)
	require.NoError(t, <-passErr)
	assert.EqualValues(t, 2, u.PutAttempts(PutObjectParquet), "PutAttempts(parquet) after the pass")
	assert.EqualValues(t, 1, u.PutFailures(PutObjectParquet, PutFailureTransient), "PutFailures(parquet, transient) after the pass")
	assert.EqualValues(t, 1, u.PutAttempts(PutObjectManifest), "PutAttempts(manifest) after the pass")
	assert.EqualValues(t, 1, u.CountersSnapshot().UploadedFiles, "CountersSnapshot().UploadedFiles after the pass")
}
