package hotstore

// №24: the partition handle cache is a bounded LRU with a capped connection
// pool. №15: CallsPage pushes the /calls filters and the page bound into SQL
// while keeping tie groups whole.

import (
	"fmt"
	"runtime"
	"sync"
	"testing"

	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func indexRow(pod string, tsMs int64, seq int, durationMs int, errorFlag bool) CallIndexRow {
	return CallIndexRow{
		PodRestart: fmt.Sprintf("ns/svc/%s/%d", pod, janitorCallTs), TraceFileIndex: seq,
		BufferOffset: 0, RecordIndex: 0, TsMs: tsMs, DurationMs: durationMs,
		RetentionClass: RetentionShortClean, ErrorFlag: errorFlag, CallsWalOffset: int64(seq),
	}
}

// TestPartitionCacheLRU pins №24: at most PartitionCacheSize handles stay
// open, the least-recently-used one closes first, and an evicted bucket's
// rows stay readable through a reopened handle.
func TestPartitionCacheLRU(t *testing.T) {
	store, err := Open(Config{DataDir: t.TempDir(), PartitionCacheSize: 2})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	buckets := make([]int64, 4)
	for i := range buckets {
		buckets[i] = store.cfg.Bucket(janitorCallTs) + int64(i)
		require.NoError(t, store.db.InsertCall(buckets[i],
			indexRow("pod-l", store.cfg.BucketStartMs(buckets[i]), i+1, 10, false)))
		store.db.mu.Lock()
		open := len(store.db.parts)
		store.db.mu.Unlock()
		assert.LessOrEqual(t, open, 2, "the handle cache never exceeds PartitionCacheSize")
	}

	for _, bucket := range buckets {
		rows, err := store.Calls(bucket)
		require.NoError(t, err)
		assert.Len(t, rows, 1, "an evicted bucket reopens and still holds its row")
	}

	require.NoError(t, store.db.withPartition(buckets[0], func(db *gorm.DB) error {
		sqlDb, err := db.DB()
		require.NoError(t, err)
		assert.Equal(t, 2, sqlDb.Stats().MaxOpenConnections, "the partition pool is capped (№24)")
		return nil
	}))
}

// TestPartitionCacheKeepsBorrowedHandlesOpen pins PR 708 review #4: a handle
// evicted while a worker is mid-query is not closed under it. Many goroutines
// each churn a distinct bucket through a cache far smaller than their count, so
// eviction constantly unlinks borrowed handles; before the ref-counted defer
// this surfaced as "sql: database is closed".
func TestPartitionCacheKeepsBorrowedHandlesOpen(t *testing.T) {
	store, err := Open(Config{DataDir: t.TempDir(), PartitionCacheSize: 2})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	const n = 8
	buckets := make([]int64, n)
	for i := range buckets {
		buckets[i] = store.cfg.Bucket(janitorCallTs) + int64(i)
		require.NoError(t, store.db.InsertCall(buckets[i],
			indexRow("pod-b", store.cfg.BucketStartMs(buckets[i]), i+1, 10, false)))
	}

	var wg sync.WaitGroup
	errs := make(chan error, n)
	for _, bucket := range buckets {
		wg.Add(1)
		go func(b int64) {
			defer wg.Done()
			for r := 0; r < 60; r++ {
				if err := store.db.withPartition(b, func(db *gorm.DB) error {
					// Hold the handle across two queries with a yield between, so
					// another goroutine's acquire evicts this bucket mid-borrow.
					var min *int64
					if err := db.Raw(`SELECT MIN(ts_ms) FROM call_index`).Scan(&min).Error; err != nil {
						return err
					}
					runtime.Gosched()
					return db.Raw(`SELECT MAX(ts_ms) FROM call_index`).Scan(&min).Error
				}); err != nil {
					errs <- err
					return
				}
			}
		}(bucket)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("a borrowed partition was closed under eviction: %v", err)
	}
}

// legacyPartitionSchema is the call_index shape before the method_text column
// shipped — what a partition file written by a pre-upgrade collector holds.
const legacyPartitionSchema = `
CREATE TABLE IF NOT EXISTS call_index (
  pod_restart      TEXT NOT NULL,
  trace_file_index INTEGER NOT NULL,
  buffer_offset    INTEGER NOT NULL,
  record_index     INTEGER NOT NULL,
  ts_ms            INTEGER NOT NULL,
  duration_ms      INTEGER NOT NULL,
  method_id        INTEGER NOT NULL,
  thread_name      TEXT NOT NULL,
  retention_class  TEXT NOT NULL,
  error_flag       INTEGER NOT NULL,
  cpu_time_ms      INTEGER NOT NULL DEFAULT 0,
  wait_time_ms     INTEGER NOT NULL DEFAULT 0,
  memory_used      INTEGER NOT NULL DEFAULT 0,
  queue_wait_ms    INTEGER NOT NULL DEFAULT 0,
  suspend_ms       INTEGER NOT NULL DEFAULT 0,
  child_calls      INTEGER NOT NULL DEFAULT 0,
  transactions     INTEGER NOT NULL DEFAULT 0,
  logs_generated   INTEGER NOT NULL DEFAULT 0,
  logs_written     INTEGER NOT NULL DEFAULT 0,
  file_read        INTEGER NOT NULL DEFAULT 0,
  file_written     INTEGER NOT NULL DEFAULT 0,
  net_read         INTEGER NOT NULL DEFAULT 0,
  net_written      INTEGER NOT NULL DEFAULT 0,
  params_json      TEXT,
  calls_wal_offset INTEGER NOT NULL,
  blob_size        INTEGER,
  truncated_reason TEXT,
  PRIMARY KEY (pod_restart, trace_file_index, buffer_offset, record_index)
);
`

// TestPartitionMigratesMethodText pins the nullable-TEXT migration path: a
// partition file written before the method_text column exists gets the column
// via the dedicated ALTER (the shared loop only adds INTEGER NOT NULL DEFAULT
// 0 columns), and every read path selects it afterwards.
func TestPartitionMigratesMethodText(t *testing.T) {
	store, err := Open(Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()
	bucket := store.cfg.Bucket(janitorCallTs)

	legacy, err := openSqlite(store.db.partitionPath(bucket))
	require.NoError(t, err)
	require.NoError(t, legacy.Exec(legacyPartitionSchema).Error)
	require.NoError(t, legacy.Exec(`INSERT INTO call_index
		(pod_restart, trace_file_index, buffer_offset, record_index, ts_ms, duration_ms,
		 method_id, thread_name, retention_class, error_flag, calls_wal_offset)
		VALUES (?, 1, 0, 0, ?, 10, 3, 'main', ?, 0, 0)`,
		"ns/svc/pod-m/1", janitorCallTs, RetentionShortClean).Error)
	sqlDb, err := legacy.DB()
	require.NoError(t, err)
	require.NoError(t, sqlDb.Close())

	rows, err := store.Calls(bucket)
	require.NoError(t, err)
	require.Len(t, rows, 1, "the pre-upgrade row survives the migration")
	assert.Empty(t, rows[0].MethodText)

	unresolved, err := store.db.BackfillMethodText("ns/svc/pod-m/1",
		func(id int) (string, bool) { return fmt.Sprintf("word-%d", id), true })
	require.NoError(t, err)
	assert.Zero(t, unresolved)

	row, ok, err := store.FindCall(PodRestartKey{Namespace: "ns", Service: "svc", PodName: "pod-m", RestartTimeMs: 1}, 1, 0, 0)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "word-3", row.MethodText, "FindCall selects the migrated column")

	page, err := store.CallsPage(bucket, model.CallsQuery{FromMs: janitorCallTs - 1, ToMs: janitorCallTs + 1},
		janitorCallTs-1, janitorCallTs+1, 10)
	require.NoError(t, err)
	require.Len(t, page, 1)
	assert.Equal(t, "word-3", page[0].MethodText, "CallsPage selects the migrated column")
}

// TestCallsPageFiltersAndLimit pins №15: the SQL page carries the pushable
// filters, returns the newest `limit` ts values WITH complete tie groups,
// and deeper pages resume below the boundary.
func TestCallsPageFiltersAndLimit(t *testing.T) {
	store, err := Open(Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()
	bucket := store.cfg.Bucket(janitorCallTs)
	base := store.cfg.BucketStartMs(bucket)

	// Three rows tie at base+100; older rows at +99 / +98; one errored row of
	// another pod at +97.
	require.NoError(t, store.db.InsertCall(bucket, indexRow("pod-a", base+100, 1, 50, false)))
	require.NoError(t, store.db.InsertCall(bucket, indexRow("pod-a", base+100, 2, 500, false)))
	require.NoError(t, store.db.InsertCall(bucket, indexRow("pod-b", base+100, 3, 50, false)))
	require.NoError(t, store.db.InsertCall(bucket, indexRow("pod-a", base+99, 4, 50, false)))
	require.NoError(t, store.db.InsertCall(bucket, indexRow("pod-a", base+98, 5, 50, false)))
	require.NoError(t, store.db.InsertCall(bucket, indexRow("pod-b", base+97, 6, 50, true)))

	window := model.CallsQuery{FromMs: base, ToMs: base + 1000}
	page, err := store.CallsPage(bucket, window, window.FromMs, window.ToMs, 2)
	require.NoError(t, err)
	assert.Len(t, page, 3, "the boundary tie group comes back whole")
	for _, row := range page {
		assert.EqualValues(t, base+100, row.TsMs)
	}

	next, err := store.CallsPage(bucket, window, window.FromMs, page[len(page)-1].TsMs, 2)
	require.NoError(t, err)
	require.Len(t, next, 2, "the next page resumes strictly below the boundary")
	assert.EqualValues(t, base+99, next[0].TsMs)
	assert.EqualValues(t, base+98, next[1].TsMs)

	filtered, err := store.CallsPage(bucket,
		model.CallsQuery{FromMs: base, ToMs: base + 1000, DurationMinMs: 100},
		base, base+1000, 10)
	require.NoError(t, err)
	require.Len(t, filtered, 1, "the duration filter runs in SQL")
	assert.Equal(t, 2, filtered[0].TraceFileIndex)

	errored, err := store.CallsPage(bucket,
		model.CallsQuery{FromMs: base, ToMs: base + 1000, ErrorOnly: true},
		base, base+1000, 10)
	require.NoError(t, err)
	require.Len(t, errored, 1)
	assert.Equal(t, 6, errored[0].TraceFileIndex)

	byPod, err := store.CallsPage(bucket,
		model.CallsQuery{FromMs: base, ToMs: base + 1000, Pods: []string{"ns/svc/pod-b"}},
		base, base+1000, 10)
	require.NoError(t, err)
	require.Len(t, byPod, 2, "the pod filter matches by pod_restart prefix")
	for _, row := range byPod {
		assert.Contains(t, row.PodRestart, "/pod-b/")
	}
	// A pod id that PREFIXES another must not leak the longer pod's rows.
	prefixPod, err := store.CallsPage(bucket,
		model.CallsQuery{FromMs: base, ToMs: base + 1000, Pods: []string{"ns/svc/pod"}},
		base, base+1000, 10)
	require.NoError(t, err)
	assert.Empty(t, prefixPod, "prefix matching stops at the '/' separator")
}
