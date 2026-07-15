package hotstore

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
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

// TestQuarantineStats pins the stuck-quarantine gauges: counts and oldest
// failure times across both quarantine kinds, empty when nothing is stuck.
func TestQuarantineStats(t *testing.T) {
	store, err := Open(Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	empty, err := store.QuarantineStats()
	require.NoError(t, err)
	assert.Zero(t, empty.ParquetCount)
	assert.Nil(t, empty.ParquetOldestMs)
	assert.Zero(t, empty.SnapshotCount)
	assert.Nil(t, empty.SnapshotOldestMs)

	key := PodRestartKey{Namespace: "ns", Service: "svc", PodName: "pod-q", RestartTimeMs: janitorCallTs}
	require.NoError(t, store.db.UpsertPodRestart(key, janitorCallTs))
	bucket := store.cfg.Bucket(janitorCallTs)
	older := seedSealedFile(t, store, key, bucket, 0)
	newer := seedSealedFile(t, store, key, bucket, 1)
	require.NoError(t, store.db.MarkUploadFailed(older, older+".failed", janitorCallTs+minute))
	require.NoError(t, store.db.MarkUploadFailed(newer, newer+".failed", janitorCallTs+2*minute))
	require.NoError(t, store.db.SetDictUploadFailed(key.String(), janitorCallTs+3*minute))

	stats, err := store.QuarantineStats()
	require.NoError(t, err)
	assert.EqualValues(t, 2, stats.ParquetCount)
	require.NotNil(t, stats.ParquetOldestMs)
	assert.Equal(t, janitorCallTs+minute, *stats.ParquetOldestMs, "oldest failure wins")
	assert.EqualValues(t, 1, stats.SnapshotCount)
	require.NotNil(t, stats.SnapshotOldestMs)
	assert.Equal(t, janitorCallTs+3*minute, *stats.SnapshotOldestMs)
}

// TestPutWithRetryCountsFailures pins the FailedPuts semantics: every failed
// attempt counts (the upload-failure-rate alert reads its rate), while
// RetriedPuts counts only attempts a retry followed.
func TestPutWithRetryCountsFailures(t *testing.T) {
	store, err := Open(Config{DataDir: t.TempDir(), UploadRetryBaseDelay: time.Millisecond})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()
	u := NewUploader(store, nil)

	var stats UploadStats
	attempts := 0
	err = u.putWithRetry(context.Background(), "k", func() error {
		attempts++
		if attempts <= 2 {
			return errors.New("transient")
		}
		return nil
	}, &stats)
	require.NoError(t, err)
	assert.EqualValues(t, 2, stats.FailedPuts)
	assert.EqualValues(t, 2, stats.RetriedPuts)

	stats = UploadStats{}
	err = u.putWithRetry(context.Background(), "k", func() error {
		return &PermanentUploadError{Err: errors.New("403")}
	}, &stats)
	require.Error(t, err)
	assert.EqualValues(t, 1, stats.FailedPuts, "a permanent rejection is still a failed PUT")
	assert.Zero(t, stats.RetriedPuts, "no retry follows a permanent rejection")
}
