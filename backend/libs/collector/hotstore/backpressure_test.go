package hotstore

// №2 flow-control: the pending-upload budget pauses seal then ingest, the
// quarantine is capped and slowly re-tested, and an S3 outage leaves the
// backlog bounded with no silent call loss. №25: the upload pass runs a
// bounded PUT pool.

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/protocol/data"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeObjectStore is an in-test S3: it can fail every PUT (a 5xx outage),
// reject permanently, and it tracks the concurrent PUT high-water mark.
type fakeObjectStore struct {
	failTransient atomic.Bool
	failPermanent atomic.Bool
	putDelay      time.Duration

	puts          atomic.Int64
	concurrent    atomic.Int64
	maxConcurrent atomic.Int64
}

func (f *fakeObjectStore) put() error {
	cur := f.concurrent.Add(1)
	defer f.concurrent.Add(-1)
	for {
		max := f.maxConcurrent.Load()
		if cur <= max || f.maxConcurrent.CompareAndSwap(max, cur) {
			break
		}
	}
	if f.putDelay > 0 {
		time.Sleep(f.putDelay)
	}
	f.puts.Add(1)
	if f.failPermanent.Load() {
		return &PermanentUploadError{Err: errors.New("403 forbidden")}
	}
	if f.failTransient.Load() {
		return errors.New("503 slow down")
	}
	return nil
}

func (f *fakeObjectStore) PutFile(context.Context, string, string) error { return f.put() }
func (f *fakeObjectStore) PutBytes(context.Context, string, []byte) error {
	return f.put()
}

// seedPendingParquet records one pending sealed file with a chosen file_size
// (the backpressure math reads the column, not the disk).
func seedPendingParquet(t *testing.T, store *Store, key PodRestartKey, seq int, fileSize int64) string {
	t.Helper()
	path := fmt.Sprintf("%s/pending-%d.parquet", store.cfg.DataDir, seq)
	require.NoError(t, os.WriteFile(path, []byte("parquet"), 0o644))
	require.NoError(t, store.db.RecordSealedFile(parquetLocalRow{
		Path: path, PodRestart: key.String(), TimeBucketMs: 0,
		RetentionClass: RetentionShortClean, Seq: seq, RowCount: 1,
		TimeMinMs: 1, TimeMaxMs: 2, FileSize: fileSize, SealedAtMs: janitorCallTs,
		S3Key: fmt.Sprintf("parquet/v1/short_clean/x/pending-%d.parquet", seq),
	}, nil))
	return path
}

// TestBackpressureGateThresholds pins the two-stage №2 budget: pending
// parquet alone trips the seal gate at half the budget, the whole backlog
// trips the ingest gate at the full budget, and draining uploads lifts both.
func TestBackpressureGateThresholds(t *testing.T) {
	ctx := context.Background()
	store, err := Open(Config{DataDir: t.TempDir(), PendingUploadMaxBytes: 1000})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()
	key := PodRestartKey{Namespace: "ns", Service: "svc", PodName: "pod-g", RestartTimeMs: janitorCallTs}
	require.NoError(t, store.db.UpsertPodRestart(key, janitorCallTs))

	refresh := func() {
		require.NoError(t, store.refreshBackpressure(ctx))
	}
	pathA := seedPendingParquet(t, store, key, 0, 400)
	refresh()
	assert.False(t, store.SealPaused(), "400 of a 500 seal threshold")
	assert.False(t, store.IngestPaused())

	pathB := seedPendingParquet(t, store, key, 1, 200)
	refresh()
	assert.True(t, store.SealPaused(), "600 pending parquet ≥ half the 1000 budget")
	assert.False(t, store.IngestPaused(), "the full budget is not reached yet")

	pathC := seedPendingParquet(t, store, key, 2, 400)
	refresh()
	assert.True(t, store.SealPaused())
	assert.True(t, store.IngestPaused(), "1000 ≥ the full budget stops accept")

	parquetBytes, partitionBytes, budget := store.PendingUploadUsage()
	assert.EqualValues(t, 1000, parquetBytes)
	assert.EqualValues(t, 0, partitionBytes)
	assert.EqualValues(t, 1000, budget)

	now := time.Now().UnixMilli()
	for _, p := range []string{pathA, pathB, pathC} {
		require.NoError(t, store.db.MarkUploaded(p, now))
	}
	refresh()
	assert.False(t, store.SealPaused(), "confirmed uploads lift the gates")
	assert.False(t, store.IngestPaused())
}

// TestS3DownBoundedBacklogNoSilentLoss is the №2 acceptance test: with S3
// answering 5xx, `parquet_local WHERE uploaded_at IS NULL` grows BOUNDED
// (the seal gate stops producing pending files), ingest engages, and no call
// is lost — every indexed call stays visible. Once S3 recovers, the backlog
// drains, sealing resumes, and the gates lift.
func TestS3DownBoundedBacklogNoSilentLoss(t *testing.T) {
	ctx := context.Background()
	store, err := Open(Config{
		DataDir:               t.TempDir(),
		PendingUploadMaxBytes: 10_000, // one fat sealed file trips the 5 KB seal gate
		UploadRetryAttempts:   1,
		UploadRetryBaseDelay:  time.Millisecond,
	})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()
	s3 := &fakeObjectStore{}
	s3.failTransient.Store(true)
	uploader := NewUploader(store, s3)

	// Six pod-restarts, one call each, in the same long-past bucket. The fat
	// incompressible param keeps every sealed file above the seal threshold.
	fat := make([]byte, 10_000)
	_, err = rand.Read(fat)
	require.NoError(t, err)
	appended := 0
	for p := 0; p < 6; p++ {
		pr, err := store.OpenPodRestart(PodRestartKey{
			Namespace: "ns", Service: "svc",
			PodName: fmt.Sprintf("pod-%d", p), RestartTimeMs: int64(p + 1)})
		require.NoError(t, err)
		_, err = pr.AppendDictionaryWord("com.example.Api.get")
		require.NoError(t, err)
		require.NoError(t, pr.AppendCall(janitorCallTs, data.Call{
			Method: 0, Duration: 10, ThreadName: "main",
			TraceFileIndex: 1, BufferOffset: 0, RecordIndex: 0,
			Params: map[data.TagId][]string{0: {hex.EncodeToString(fat)}},
		}))
		appended++
	}
	bucket := store.cfg.Bucket(janitorCallTs)
	indexedCalls := func() int {
		rows, err := store.Calls(bucket)
		require.NoError(t, err)
		return len(rows)
	}
	backlog := func() int64 {
		n, _, err := store.UploadBacklog()
		require.NoError(t, err)
		return n
	}

	now := time.Now().UnixMilli()
	var backlogs []int64
	sealedTotal := 0
	for round := 0; round < 4; round++ {
		sealed, err := store.SealDue(ctx, now)
		require.NoError(t, err)
		sealedTotal += sealed
		_, err = uploader.Pass(ctx)
		require.NoError(t, err)
		backlogs = append(backlogs, backlog())
	}
	require.Greater(t, sealedTotal, 0, "sealing runs until the gate trips")
	assert.Less(t, sealedTotal, 6, "backpressure stops sealing before the whole backlog turns into pending parquet")
	assert.True(t, store.SealPaused())
	assert.Equal(t, backlogs[len(backlogs)-2], backlogs[len(backlogs)-1],
		"with the gate engaged, further rounds do not grow the pending backlog")
	assert.Equal(t, appended, indexedCalls(), "no call is silently lost while S3 is down")
	// The call partitions on disk push the whole backlog over the full budget.
	require.NoError(t, store.refreshBackpressure(ctx))
	assert.True(t, store.IngestPaused(), "the full backlog stops accept")

	quarantine, err := store.QuarantineStats()
	require.NoError(t, err)
	assert.Zero(t, quarantine.ParquetCount, "a transient 5xx never quarantines")

	// S3 recovers: uploads drain, the seal gate lifts, the rest seals.
	s3.failTransient.Store(false)
	for round := 0; round < 10 && (backlog() > 0 || store.SealQueueDepth() > 0); round++ {
		_, err = uploader.Pass(ctx)
		require.NoError(t, err)
		_, err = store.SealDue(ctx, now)
		require.NoError(t, err)
	}
	_, err = uploader.Pass(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 0, backlog(), "the backlog drains once S3 recovers")
	assert.False(t, store.SealPaused())
	assert.Equal(t, appended, indexedCalls(), "every call survived the outage")
}

// TestQuarantineRetestRecovers pins the №2 slow re-test: a permanent
// rejection quarantines the file, and after the retest interval the next
// pass retries it — successfully once the rejection healed.
func TestQuarantineRetestRecovers(t *testing.T) {
	ctx := context.Background()
	store, err := Open(Config{
		DataDir:                  t.TempDir(),
		UploadRetryAttempts:      1,
		UploadRetryBaseDelay:     time.Millisecond,
		QuarantineRetestInterval: time.Millisecond,
	})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()
	s3 := &fakeObjectStore{}
	s3.failPermanent.Store(true)
	uploader := NewUploader(store, s3)

	key := PodRestartKey{Namespace: "ns", Service: "svc", PodName: "pod-r", RestartTimeMs: janitorCallTs}
	require.NoError(t, store.db.UpsertPodRestart(key, janitorCallTs))
	seedPendingParquet(t, store, key, 0, 100)

	stats, err := uploader.Pass(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 1, stats.QuarantinedFiles)
	q, err := store.QuarantineStats()
	require.NoError(t, err)
	require.EqualValues(t, 1, q.ParquetCount)

	// The rejection heals (credentials rotated back); the retest interval
	// elapses and the next pass re-queues and uploads the file.
	s3.failPermanent.Store(false)
	time.Sleep(5 * time.Millisecond)
	stats, err = uploader.Pass(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 1, stats.RequeuedFiles)
	assert.EqualValues(t, 1, stats.UploadedFiles)
	q, err = store.QuarantineStats()
	require.NoError(t, err)
	assert.Zero(t, q.ParquetCount, "a successful re-test empties the quarantine")
	backlog, _, err := store.UploadBacklog()
	require.NoError(t, err)
	assert.Zero(t, backlog)
}

// TestJanitorQuarantineCap pins the №2 age/size cap: quarantined parquet
// past the age cap (or the oldest of it over the size cap) is dropped with
// its row, releasing what it pinned.
func TestJanitorQuarantineCap(t *testing.T) {
	ctx := context.Background()
	store, err := Open(Config{
		DataDir:            t.TempDir(),
		QuarantineMaxAge:   time.Hour,
		QuarantineMaxBytes: 250,
	})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()
	key := PodRestartKey{Namespace: "ns", Service: "svc", PodName: "pod-c", RestartTimeMs: janitorCallTs}
	require.NoError(t, store.db.UpsertPodRestart(key, janitorCallTs))
	require.NoError(t, store.db.ClosePodRestart(key, janitorCallTs))

	now := time.Now().UnixMilli()
	hour := time.Hour.Milliseconds()
	quarantined := func(seq int, failedAtMs int64, size int) string {
		path := seedPendingParquet(t, store, key, seq, int64(size))
		require.NoError(t, os.WriteFile(path, make([]byte, size), 0o644))
		require.NoError(t, store.db.MarkUploadFailed(path, path, failedAtMs))
		return path
	}
	// A is over the age cap; B and C are fresh but 300 bytes together, over
	// the 250 size cap, so the older of them (B) goes too.
	pathA := quarantined(0, now-2*hour, 100)
	pathB := quarantined(1, now-2*time.Minute.Milliseconds(), 200)
	pathC := quarantined(2, now-time.Minute.Milliseconds(), 100)

	stats, err := store.JanitorPass(ctx, now)
	require.NoError(t, err)
	assert.EqualValues(t, 2, stats.QuarantineDropped)
	assert.NoFileExists(t, pathA, "aged out")
	assert.NoFileExists(t, pathB, "the oldest fresh file goes to fit the size cap")
	assert.FileExists(t, pathC, "the rest fits the cap")

	q, err := store.QuarantineStats()
	require.NoError(t, err)
	assert.EqualValues(t, 1, q.ParquetCount)
}

// TestUploaderPoolBoundedConcurrency pins №25: pending files upload over a
// bounded worker pool — parallel, but never wider than UploadConcurrency.
func TestUploaderPoolBoundedConcurrency(t *testing.T) {
	ctx := context.Background()
	store, err := Open(Config{DataDir: t.TempDir(), UploadConcurrency: 3})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()
	s3 := &fakeObjectStore{putDelay: 20 * time.Millisecond}
	uploader := NewUploader(store, s3)

	key := PodRestartKey{Namespace: "ns", Service: "svc", PodName: "pod-p", RestartTimeMs: janitorCallTs}
	require.NoError(t, store.db.UpsertPodRestart(key, janitorCallTs))
	for seq := 0; seq < 9; seq++ {
		seedPendingParquet(t, store, key, seq, 100)
	}

	stats, err := uploader.Pass(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 9, stats.UploadedFiles)
	backlog, _, err := store.UploadBacklog()
	require.NoError(t, err)
	assert.Zero(t, backlog)
	assert.LessOrEqual(t, s3.maxConcurrent.Load(), int64(3), "the pool never exceeds UploadConcurrency")
	assert.GreaterOrEqual(t, s3.maxConcurrent.Load(), int64(2), "the pool actually runs PUTs in parallel")
}
