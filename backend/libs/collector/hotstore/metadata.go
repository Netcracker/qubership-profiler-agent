package hotstore

import (
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	"github.com/glebarez/sqlite"
	"github.com/pkg/errors"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// metadata.sqlite schema per 03-lifecycle.md §3.2. parquet_segments maps each
// sealed file to the segments its rows source from, so the S3 upload task can
// decrement the refcounts the seal pass added (01-write-contract.md §6.2
// step 3). upload_failed_at quarantines a file S3 rejected with a permanent
// error: the row keeps its refcounts pinned but leaves the upload queue
// (01 §8, upload-failed/).
const metaSchema = `
CREATE TABLE IF NOT EXISTS pod_restarts (
  pod_restart      TEXT PRIMARY KEY,
  namespace        TEXT NOT NULL,
  service          TEXT NOT NULL,
  pod_name         TEXT NOT NULL,
  restart_time_ms  INTEGER NOT NULL,
  opened_at        INTEGER NOT NULL,
  closed_at        INTEGER,
  wals_purged_at   INTEGER
);
CREATE TABLE IF NOT EXISTS segments (
  pod_restart  TEXT NOT NULL,
  stream       TEXT NOT NULL,
  rolling_seq  INTEGER NOT NULL,
  path         TEXT NOT NULL,
  logical_size INTEGER NOT NULL DEFAULT 0,
  time_min_ms  INTEGER,
  time_max_ms  INTEGER,
  refcount     INTEGER NOT NULL DEFAULT 0,
  status       TEXT NOT NULL DEFAULT 'open',
  created_at   INTEGER NOT NULL,
  evicted_at   INTEGER,
  PRIMARY KEY (pod_restart, stream, rolling_seq)
);
CREATE TABLE IF NOT EXISTS parquet_local (
  path            TEXT PRIMARY KEY,
  pod_restart     TEXT NOT NULL,
  time_bucket_ms  INTEGER NOT NULL,
  retention_class TEXT NOT NULL,
  seq             INTEGER NOT NULL,
  row_count       INTEGER NOT NULL,
  time_min_ms     INTEGER NOT NULL,
  time_max_ms     INTEGER NOT NULL,
  file_size       INTEGER NOT NULL,
  sealed_at       INTEGER NOT NULL,
  wal_offset_lo   INTEGER NOT NULL DEFAULT 0,
  uploaded_at     INTEGER,
  upload_failed_at INTEGER,
  first_failed_at  INTEGER,
  s3_key          TEXT
);
CREATE TABLE IF NOT EXISTS parquet_segments (
  path        TEXT NOT NULL,
  pod_restart TEXT NOT NULL,
  stream      TEXT NOT NULL,
  rolling_seq INTEGER NOT NULL,
  row_count   INTEGER NOT NULL,
  PRIMARY KEY (path, pod_restart, stream, rolling_seq)
);
CREATE TABLE IF NOT EXISTS seal_state (
  pod_restart     TEXT NOT NULL,
  bucket          INTEGER NOT NULL,
  retention_class TEXT NOT NULL,
  watermark       INTEGER NOT NULL DEFAULT 0,
  last_sealed_at  INTEGER,
  dirty           INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (pod_restart, bucket, retention_class)
);
CREATE TABLE IF NOT EXISTS call_partitions (
  bucket     INTEGER PRIMARY KEY,
  path       TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  dropped_at INTEGER
);
`

// Per-bucket call index (03-lifecycle.md §3.2), one SQLite file per time
// bucket so eviction drops a partition instead of running a large DELETE.
const partitionSchema = `
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
  method_text      TEXT,
  PRIMARY KEY (pod_restart, trace_file_index, buffer_offset, record_index)
);
CREATE INDEX IF NOT EXISTS call_index_ts_ms ON call_index (ts_ms);
CREATE INDEX IF NOT EXISTS call_index_duration_ms ON call_index (duration_ms);
CREATE INDEX IF NOT EXISTS call_index_method_id ON call_index (method_id);
`

type (
	// CallIndexRow is one call_index row: the read contract's Call PK plus the
	// filter columns that answer a hot query without touching the blob.
	CallIndexRow struct {
		PodRestart     string
		TraceFileIndex int
		BufferOffset   int
		RecordIndex    int
		TsMs           int64
		DurationMs     int
		MethodId       int
		ThreadName     string
		RetentionClass string
		ErrorFlag      bool
		CpuTimeMs      int64
		WaitTimeMs     int64
		MemoryUsed     int64
		QueueWaitMs    int
		SuspendMs      int
		ChildCalls     int
		Transactions   int
		LogsGenerated  int64
		LogsWritten    int64
		FileRead       int64
		FileWritten    int64
		NetRead        int64
		NetWritten     int64
		ParamsJson     string
		CallsWalOffset int64
		// MethodText is the resolved method name the fast-path WAL purge
		// materializes into the row before the pod-restart's dictionary WAL is
		// deleted (janitor.go purgeWals); empty until then. The hot read path
		// prefers it over a DictWord lookup, so /calls keeps rendering names for
		// a fast-purged pod-restart whose in-RAM state is gone.
		MethodText string
	}

	// SegmentRow mirrors one segments-catalog row.
	SegmentRow struct {
		PodRestart  string
		Stream      string
		RollingSeq  int
		Path        string
		LogicalSize int64
		TimeMinMs   *int64
		TimeMaxMs   *int64
		Refcount    int
		Status      string
	}

	// parquetLocalRow mirrors one parquet_local row: a sealed file held
	// locally, pending upload while uploaded_at is NULL (03-lifecycle.md §3.2).
	parquetLocalRow struct {
		Path           string
		PodRestart     string
		TimeBucketMs   int64
		RetentionClass string
		Seq            int
		RowCount       int
		TimeMinMs      int64
		TimeMaxMs      int64
		FileSize       int64
		SealedAtMs     int64
		// WalOffsetLo is the seal pass's start watermark — the first calls.wal
		// offset the pass covered. It bounds the re-seal rewind when this file's
		// local copy is lost before upload (recovery.go reconcileParquetLocal):
		// rewinding the bucket watermark here re-exposes exactly this pass's
		// calls (and any sealed after it), never the ones sealed before.
		WalOffsetLo int64
		S3Key       string
	}

	// ParquetLocalFile is the exported view of parquet_local for tests and the
	// upload task.
	ParquetLocalFile struct {
		Path             string
		PodRestart       string
		TimeBucketMs     int64
		RetentionClass   string
		Seq              int
		RowCount         int
		TimeMinMs        int64
		TimeMaxMs        int64
		FileSize         int64
		UploadedAtMs     *int64
		UploadFailedAtMs *int64
		FirstFailedAtMs  *int64
		S3Key            string
	}

	// partHandle is one cached partition DB plus the LRU bookkeeping: a recency
	// tick to pick the eviction victim, a live-borrow count, and an evicted
	// flag. A handle evicted while still borrowed is unlinked from the cache but
	// its Close is deferred to the last release, so a seal/upload/ingest worker
	// mid-query never races into "sql: database is closed" (PR 708 review #4).
	partHandle struct {
		db      *gorm.DB
		useTick int64
		refs    int
		evicted bool
	}

	// metaDb wraps metadata.sqlite plus the per-bucket partition handles.
	// parts is an LRU of at most cfg.PartitionCacheSize open handles (№24);
	// each handle carries the recency tick that picks the eviction victim.
	metaDb struct {
		cfg  Config
		meta *gorm.DB

		mu      sync.Mutex
		parts   map[int64]*partHandle
		useTick int64
	}
)

func openSqlite(path string) (*gorm.DB, error) {
	db, err := gorm.Open(sqlite.Open(path), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		return nil, errors.Wrapf(err, "open sqlite %s", path)
	}
	sqlDb, err := db.DB()
	if err != nil {
		return nil, errors.Wrap(err, "unwrap sqlite handle")
	}
	for _, pragma := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=NORMAL",
		"PRAGMA busy_timeout=5000",
	} {
		if _, err := sqlDb.Exec(pragma); err != nil {
			return nil, errors.Wrapf(err, "exec %s on %s", pragma, path)
		}
	}
	return db, nil
}

func openMetaDb(cfg Config) (*metaDb, error) {
	db, err := openSqlite(filepath.Join(cfg.DataDir, "metadata.sqlite"))
	if err != nil {
		return nil, err
	}
	var integrity string
	if err := db.Raw("PRAGMA integrity_check").Scan(&integrity).Error; err != nil || integrity != "ok" {
		// 03-lifecycle.md §3.2 step 4 repairs by rebuilding from PV contents;
		// until that lands, surface the corruption as FATAL.
		return nil, fmt.Errorf("metadata.sqlite integrity check: %q, %v", integrity, err)
	}
	if err := db.Exec(metaSchema).Error; err != nil {
		return nil, errors.Wrap(err, "migrate metadata.sqlite")
	}
	// Columns that joined the schema after their table shipped need the ALTER
	// for a metadata.sqlite created before them (new files get them via CREATE
	// TABLE above, so the duplicate-column error is the common case).
	for _, alter := range []string{
		`ALTER TABLE parquet_local ADD COLUMN upload_failed_at INTEGER`,
		`ALTER TABLE parquet_local ADD COLUMN first_failed_at INTEGER`,
		// A row written before this column backfills wal_offset_lo to 0. If such
		// a legacy pending file is later lost, RecoverLostPendingParquet rewinds
		// the whole bucket to offset 0 — a bounded, dedup-safe full-bucket reseal,
		// never the original silent-loss bug. Over-rewind is the right default for
		// a durability fix during the upgrade window.
		`ALTER TABLE parquet_local ADD COLUMN wal_offset_lo INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE pod_restarts ADD COLUMN wals_purged_at INTEGER`,
	} {
		if err := db.Exec(alter).Error; err != nil &&
			!strings.Contains(err.Error(), "duplicate column name") {
			return nil, errors.Wrapf(err, "migrate: %s", alter)
		}
	}
	return &metaDb{cfg: cfg, meta: db, parts: map[int64]*partHandle{}}, nil
}

func (m *metaDb) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var firstErr error
	closeGorm := func(db *gorm.DB) {
		if sqlDb, err := db.DB(); err == nil {
			if err := sqlDb.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	for _, h := range m.parts {
		closeGorm(h.db)
	}
	m.parts = map[int64]*partHandle{}
	closeGorm(m.meta)
	return firstErr
}

func (m *metaDb) UpsertPodRestart(key PodRestartKey, openedAtMs int64) error {
	return m.meta.Exec(`INSERT OR IGNORE INTO pod_restarts
		(pod_restart, namespace, service, pod_name, restart_time_ms, opened_at)
		VALUES (?, ?, ?, ?, ?, ?)`,
		key.String(), key.Namespace, key.Service, key.PodName, key.RestartTimeMs, openedAtMs).Error
}

func (m *metaDb) ClosePodRestart(key PodRestartKey, closedAtMs int64) error {
	return m.meta.Exec(`UPDATE pod_restarts SET closed_at = ? WHERE pod_restart = ? AND closed_at IS NULL`,
		closedAtMs, key.String()).Error
}

// CloseAllOpen implements recovery step 3.3: every pod-restart still marked
// open belonged to a TCP connection the crash broke, so it is closed now.
func (m *metaDb) CloseAllOpen(nowMs int64) error {
	return m.meta.Exec(`UPDATE pod_restarts SET closed_at = ? WHERE closed_at IS NULL`, nowMs).Error
}

func (m *metaDb) UpsertSegment(podRestart, stream string, seq int, path string, createdAtMs int64) error {
	return m.meta.Exec(`INSERT OR IGNORE INTO segments
		(pod_restart, stream, rolling_seq, path, created_at)
		VALUES (?, ?, ?, ?, ?)`,
		podRestart, stream, seq, path, createdAtMs).Error
}

func (m *metaDb) FinalizeSegment(podRestart, stream string, seq int, logicalSize int64, timeMinMs, timeMaxMs *int64) error {
	return m.meta.Exec(`UPDATE segments
		SET logical_size = ?, time_min_ms = ?, time_max_ms = ?, status = 'closed'
		WHERE pod_restart = ? AND stream = ? AND rolling_seq = ?`,
		logicalSize, timeMinMs, timeMaxMs, podRestart, stream, seq).Error
}

func (m *metaDb) Segments(podRestart string) ([]SegmentRow, error) {
	var rows []SegmentRow
	err := m.meta.Raw(`SELECT pod_restart, stream, rolling_seq, path, logical_size, time_min_ms, time_max_ms, refcount, status
		FROM segments WHERE pod_restart = ? ORDER BY stream, rolling_seq`, podRestart).Scan(&rows).Error
	return rows, err
}

// SealWatermark reports the first calls.wal offset the bucket's seals have not
// covered yet (01-write-contract.md §6.6). Zero means nothing is sealed.
func (m *metaDb) SealWatermark(podRestart string, bucket int64) (int64, error) {
	var watermark int64
	err := m.meta.Raw(`SELECT COALESCE(MAX(watermark), 0) FROM seal_state
		WHERE pod_restart = ? AND bucket = ?`, podRestart, bucket).Scan(&watermark).Error
	return watermark, err
}

// UpsertSealState advances one class's watermark after a seal (§6.2 step 2).
func (m *metaDb) UpsertSealState(podRestart string, bucket int64, retentionClass string, watermark, sealedAtMs int64) error {
	return m.meta.Exec(`INSERT INTO seal_state (pod_restart, bucket, retention_class, watermark, last_sealed_at, dirty)
		VALUES (?, ?, ?, ?, ?, 0)
		ON CONFLICT (pod_restart, bucket, retention_class)
		DO UPDATE SET watermark = excluded.watermark, last_sealed_at = excluded.last_sealed_at, dirty = 0`,
		podRestart, bucket, retentionClass, watermark, sealedAtMs).Error
}

// NextParquetSeq picks the <seq> of the next file for (bucket, class,
// pod-restart): patch files and size splits continue the numbering (§6.6).
func (m *metaDb) NextParquetSeq(podRestart string, timeBucketMs int64, retentionClass string) (int, error) {
	var seq int
	err := m.meta.Raw(`SELECT COALESCE(MAX(seq) + 1, 0) FROM parquet_local
		WHERE pod_restart = ? AND time_bucket_ms = ? AND retention_class = ?`,
		podRestart, timeBucketMs, retentionClass).Scan(&seq).Error
	return seq, err
}

// RecordSealedFile registers one sealed parquet file and pins its source
// segments: refcount += rows per segment, with the per-file mapping kept in
// parquet_segments so the upload task can decrement the same amounts
// (01-write-contract.md §6.2, 03-lifecycle.md §3.2). The seal pass itself
// commits through CommitSealPass; this single-file form remains for callers
// that record a file with no watermark to move (tests, future repair paths).
func (m *metaDb) RecordSealedFile(row parquetLocalRow, segRows map[segKey]int) error {
	return m.meta.Transaction(func(tx *gorm.DB) error {
		return recordSealedFileTx(tx, row, segRows)
	})
}

// recordSealedFileTx is the shared body of RecordSealedFile and
// CommitSealPass; it must run inside the caller's transaction.
func recordSealedFileTx(tx *gorm.DB, row parquetLocalRow, segRows map[segKey]int) error {
	if err := tx.Exec(`INSERT INTO parquet_local
		(path, pod_restart, time_bucket_ms, retention_class, seq, row_count,
		 time_min_ms, time_max_ms, file_size, sealed_at, wal_offset_lo, uploaded_at, s3_key)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?)`,
		row.Path, row.PodRestart, row.TimeBucketMs, row.RetentionClass, row.Seq, row.RowCount,
		row.TimeMinMs, row.TimeMaxMs, row.FileSize, row.SealedAtMs, row.WalOffsetLo, row.S3Key).Error; err != nil {
		return errors.Wrap(err, "record sealed parquet")
	}
	for sk, rows := range segRows {
		if err := tx.Exec(`INSERT INTO parquet_segments (path, pod_restart, stream, rolling_seq, row_count)
			VALUES (?, ?, ?, ?, ?)`,
			row.Path, row.PodRestart, sk.stream, sk.seq, rows).Error; err != nil {
			return errors.Wrap(err, "record sealed-file segment refs")
		}
		if err := tx.Exec(`UPDATE segments SET refcount = refcount + ?
			WHERE pod_restart = ? AND stream = ? AND rolling_seq = ?`,
			rows, row.PodRestart, sk.stream, sk.seq).Error; err != nil {
			return errors.Wrap(err, "pin sealed-file segments")
		}
	}
	return nil
}

// sealCommit pairs one finished class file with its segment refcounts for the
// pass-level commit.
type sealCommit struct {
	row     parquetLocalRow
	segRows map[segKey]int
}

// CommitSealPass records every class file of one seal pass AND advances the
// bucket's watermark in a single transaction (№6). A kill -9 anywhere before
// the commit leaves no watermark and no parquet_local rows, so the retry
// re-seals the identical row set into the identical file names; a crash after
// it re-seals nothing. Splitting the watermark from the file records (the old
// per-file RecordSealedFile + separate UpsertSealState) let a crash in between
// re-seal already-recorded rows under a new seq — cross-class duplicates
// nothing downstream dedups.
func (m *metaDb) CommitSealPass(podRestart string, bucket int64, commits []sealCommit, watermark, sealedAtMs int64) error {
	return m.meta.Transaction(func(tx *gorm.DB) error {
		for _, c := range commits {
			if err := recordSealedFileTx(tx, c.row, c.segRows); err != nil {
				return err
			}
			if err := tx.Exec(`INSERT INTO seal_state (pod_restart, bucket, retention_class, watermark, last_sealed_at, dirty)
				VALUES (?, ?, ?, ?, ?, 0)
				ON CONFLICT (pod_restart, bucket, retention_class)
				DO UPDATE SET watermark = excluded.watermark, last_sealed_at = excluded.last_sealed_at, dirty = 0`,
				podRestart, bucket, c.row.RetentionClass, watermark, sealedAtMs).Error; err != nil {
				return errors.Wrap(err, "advance seal watermark")
			}
		}
		return nil
	})
}

// LocalParquet lists a pod-restart's sealed files still held locally.
func (m *metaDb) LocalParquet(podRestart string) ([]ParquetLocalFile, error) {
	var rows []ParquetLocalFile
	err := m.meta.Raw(`SELECT path, pod_restart, time_bucket_ms, retention_class, seq, row_count,
		time_min_ms, time_max_ms, file_size, uploaded_at AS uploaded_at_ms,
		upload_failed_at AS upload_failed_at_ms, first_failed_at AS first_failed_at_ms, s3_key
		FROM parquet_local WHERE pod_restart = ? ORDER BY path`, podRestart).Scan(&rows).Error
	return rows, err
}

// releaseSealedFileRefs decrements the segment refcounts a sealed file pinned
// and deletes its parquet_segments rows. Running inside one transaction with
// the caller's state change makes the release exactly-once: a second call
// finds no parquet_segments rows and decrements nothing (01-write-contract.md
// §6.2 step 3).
func releaseSealedFileRefs(tx *gorm.DB, path string) error {
	if err := tx.Exec(`UPDATE segments SET refcount = refcount - (
			SELECT ps.row_count FROM parquet_segments ps
			WHERE ps.path = ? AND ps.pod_restart = segments.pod_restart
			  AND ps.stream = segments.stream AND ps.rolling_seq = segments.rolling_seq)
		WHERE EXISTS (
			SELECT 1 FROM parquet_segments ps
			WHERE ps.path = ? AND ps.pod_restart = segments.pod_restart
			  AND ps.stream = segments.stream AND ps.rolling_seq = segments.rolling_seq)`,
		path, path).Error; err != nil {
		return errors.Wrap(err, "release sealed-file segments")
	}
	return errors.Wrap(tx.Exec(`DELETE FROM parquet_segments WHERE path = ?`, path).Error,
		"drop sealed-file segment refs")
}

// DropParquetLocal forgets a sealed file and releases the segment refs it
// pinned; used when an already-uploaded file's local copy vanished
// (03-lifecycle.md §3.6). The data is durable in S3, so no re-seal follows: the
// ref release is a no-op here because MarkUploaded already released them.
func (m *metaDb) DropParquetLocal(path string) error {
	return m.meta.Transaction(func(tx *gorm.DB) error {
		if err := releaseSealedFileRefs(tx, path); err != nil {
			return err
		}
		return errors.Wrap(tx.Exec(`DELETE FROM parquet_local WHERE path = ?`, path).Error,
			"drop sealed parquet row")
	})
}

// ParquetReconcileRow is the recovery view of one parquet_local row: enough to
// tell the durable-in-S3 case (uploaded) from the lost pending case, and to
// bound the re-seal rewind (wal_offset_lo) when the local file is gone.
type ParquetReconcileRow struct {
	Path         string
	PodRestart   string
	TimeBucketMs int64
	WalOffsetLo  int64
	Uploaded     bool
}

// ParquetLocalReconcile lists every sealed file the catalog believes exists,
// with the fields recovery needs to reconcile a missing local copy
// (03-lifecycle.md §3.6 step 10).
func (m *metaDb) ParquetLocalReconcile() ([]ParquetReconcileRow, error) {
	var raw []struct {
		Path         string
		PodRestart   string
		TimeBucketMs int64
		WalOffsetLo  int64
		UploadedAt   *int64
	}
	if err := m.meta.Raw(`SELECT path, pod_restart, time_bucket_ms, wal_offset_lo, uploaded_at
		FROM parquet_local ORDER BY path`).Scan(&raw).Error; err != nil {
		return nil, errors.Wrap(err, "list parquet_local for reconcile")
	}
	rows := make([]ParquetReconcileRow, len(raw))
	for i, r := range raw {
		rows[i] = ParquetReconcileRow{
			Path: r.Path, PodRestart: r.PodRestart, TimeBucketMs: r.TimeBucketMs,
			WalOffsetLo: r.WalOffsetLo, Uploaded: r.UploadedAt != nil,
		}
	}
	return rows, nil
}

// RecoverLostPendingParquet handles the QA 708#2 loss window: a pending (not yet
// uploaded) sealed file whose local copy vanished before it reached S3. In ONE
// transaction it rewinds the bucket's seal watermark to walOffsetLo — the start
// of the pass that produced the file — so the file's source calls re-enter the
// seal loop and rebuild a replacement, then drops the dead catalog row and
// releases its now-defunct segment pins. The rewind and the drop must be atomic:
// dropping the row without rewinding (the old behaviour) left the calls stranded
// below the watermark forever, invisible to upload and cold queries even though
// their segment data still existed. MIN keeps the rewind monotone — it never
// advances a watermark and re-running it is a no-op — so a crashed recovery
// simply repeats it. A row written before the wal_offset_lo column carries
// lo = 0, so a lost legacy pending file rewinds the whole bucket: a bounded,
// dedup-safe full-bucket reseal, never the silent-loss bug — the right default
// for a durability fix during the upgrade window.
func (m *metaDb) RecoverLostPendingParquet(path, podRestart string, bucket, walOffsetLo int64) error {
	return m.meta.Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec(`UPDATE seal_state SET watermark = MIN(watermark, ?)
			WHERE pod_restart = ? AND bucket = ?`, walOffsetLo, podRestart, bucket).Error; err != nil {
			return errors.Wrap(err, "rewind seal watermark")
		}
		if err := releaseSealedFileRefs(tx, path); err != nil {
			return err
		}
		return errors.Wrap(tx.Exec(`DELETE FROM parquet_local WHERE path = ?`, path).Error,
			"drop lost pending parquet row")
	})
}

// PendingUploads lists the sealed files still owed to S3, across pod-restarts:
// not uploaded and not quarantined (01-write-contract.md §6.2, 03 §3.8).
func (m *metaDb) PendingUploads() ([]ParquetLocalFile, error) {
	var rows []ParquetLocalFile
	err := m.meta.Raw(`SELECT path, pod_restart, time_bucket_ms, retention_class, seq, row_count,
		time_min_ms, time_max_ms, file_size, uploaded_at AS uploaded_at_ms,
		upload_failed_at AS upload_failed_at_ms, s3_key
		FROM parquet_local WHERE uploaded_at IS NULL AND upload_failed_at IS NULL
		ORDER BY path`).Scan(&rows).Error
	return rows, err
}

// PendingParquetBytes sums the file sizes of every sealed file not confirmed
// in S3 — pending or quarantined, both still on the PV — the parquet half of
// the №2 pending-upload budget.
func (m *metaDb) PendingParquetBytes() (int64, error) {
	var n int64
	err := m.meta.Raw(`SELECT COALESCE(SUM(file_size), 0) FROM parquet_local
		WHERE uploaded_at IS NULL`).Scan(&n).Error
	return n, errors.Wrap(err, "sum pending parquet bytes")
}

// PartitionPaths lists the live call-index partition files — the partition
// half of the №2 pending-upload budget; the caller stats them.
func (m *metaDb) PartitionPaths() ([]string, error) {
	var paths []string
	err := m.meta.Raw(`SELECT path FROM call_partitions WHERE dropped_at IS NULL
		ORDER BY bucket`).Scan(&paths).Error
	return paths, err
}

// QuarantinedParquet lists the quarantined sealed files, oldest FIRST failure
// first — the order the №2 quarantine cap evicts in. The order and the cap key
// off first_failed_at (the immutable first-failure time), not the re-test-reset
// upload_failed_at, so the age cap measures true failure age.
func (m *metaDb) QuarantinedParquet() ([]ParquetLocalFile, error) {
	var rows []ParquetLocalFile
	err := m.meta.Raw(`SELECT path, pod_restart, time_bucket_ms, retention_class, seq, row_count,
		time_min_ms, time_max_ms, file_size, uploaded_at AS uploaded_at_ms,
		upload_failed_at AS upload_failed_at_ms, first_failed_at AS first_failed_at_ms, s3_key
		FROM parquet_local WHERE upload_failed_at IS NOT NULL
		ORDER BY COALESCE(first_failed_at, upload_failed_at), path`).Scan(&rows).Error
	return rows, err
}

// RequeueQuarantinedParquet returns quarantined files whose last rejection is
// older than cutoffMs to the upload queue — the №2 slow re-test: a
// "permanent" rejection is often operational (expired credentials, missing
// bucket) and heals. The file stays under upload-failed/; a repeat rejection
// re-quarantines it in place.
func (m *metaDb) RequeueQuarantinedParquet(cutoffMs int64) (int64, error) {
	res := m.meta.Exec(`UPDATE parquet_local SET upload_failed_at = NULL
		WHERE upload_failed_at IS NOT NULL AND upload_failed_at <= ?`, cutoffMs)
	return res.RowsAffected, errors.Wrap(res.Error, "requeue quarantined parquet")
}

// UploadBacklog reports the pending-upload gauges in ONE query: how many
// sealed files are still owed to S3 (not uploaded, not quarantined) and the
// sealed_at of the oldest of them. oldestSealedMs is nil when the queue is
// empty. Feeds upload_backlog and upload_lag_seconds.
func (m *metaDb) UploadBacklog() (count int64, oldestSealedMs *int64, err error) {
	var row struct {
		N            int64
		OldestSealed *int64
	}
	err = m.meta.Raw(`SELECT COUNT(*) AS n, MIN(sealed_at) AS oldest_sealed
		FROM parquet_local WHERE uploaded_at IS NULL AND upload_failed_at IS NULL`).
		Scan(&row).Error
	if err != nil {
		return 0, nil, errors.Wrap(err, "read upload backlog")
	}
	return row.N, row.OldestSealed, nil
}

// MarkUploaded implements 01-write-contract.md §6.2 step 3 after a confirmed
// PUT: uploaded_at and the refcount release commit in ONE transaction, so a
// crash on either side of it re-runs the whole step. Idempotent: the release
// deletes the file's parquet_segments rows, so a repeat decrements nothing and
// a segment other rows still pin is never freed early (invariant C1).
func (m *metaDb) MarkUploaded(path string, uploadedAtMs int64) error {
	return m.meta.Transaction(func(tx *gorm.DB) error {
		if err := releaseSealedFileRefs(tx, path); err != nil {
			return err
		}
		return errors.Wrap(tx.Exec(`UPDATE parquet_local SET uploaded_at = ?
			WHERE path = ? AND uploaded_at IS NULL`, uploadedAtMs, path).Error,
			"mark sealed parquet uploaded")
	})
}

// MarkUploadFailed re-points a quarantined file at its upload-failed/ location
// and takes it out of the upload queue. The file's parquet_segments refs move
// with it in the SAME transaction: a rejected file keeps its segments pinned
// until a human resolves it (01 §8), and the refs key off parquet_local.path,
// so migrating the local path WITHOUT the segment refs would strand them — a
// later MarkUploaded(quarantinePath) or DropParquetLocal(quarantinePath) would
// match no parquet_segments rows and pin the refcounts forever (QA 708#3).
// first_failed_at is stamped once on the FIRST quarantine and never reset, so
// the give-up age cap and the stuck-quarantine metric measure the true age of
// the failure even though the slow re-test keeps rewriting upload_failed_at.
func (m *metaDb) MarkUploadFailed(path, quarantinePath string, failedAtMs int64) error {
	return m.meta.Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec(`UPDATE parquet_local
			SET path = ?, upload_failed_at = ?, first_failed_at = COALESCE(first_failed_at, ?)
			WHERE path = ? AND uploaded_at IS NULL`, quarantinePath, failedAtMs, failedAtMs, path).Error; err != nil {
			return errors.Wrap(err, "mark sealed parquet upload-failed")
		}
		// The segment refs key off the local path; move them to the quarantine
		// path so the release still finds them. When the row was already uploaded
		// (the guard above matched nothing) MarkUploaded already deleted these
		// rows, so this migrates none — the two stay consistent.
		return errors.Wrap(tx.Exec(`UPDATE parquet_segments SET path = ? WHERE path = ?`,
			quarantinePath, path).Error, "migrate quarantined segment refs")
	})
}

// QuarantineForManifest takes a sealed file OUT of the tight upload queue after
// its pod-restart manifest was permanently rejected, WITHOUT marking it
// uploaded and WITHOUT moving it. The manifest is the only cold source of the
// pod-restart's readable identity (01 §3.6, 02 §2.7), so uploaded_at must stay
// NULL until the manifest is durable: while it is NULL the pod-restart stays
// discoverable/pending, HasPendingParquet blocks the WAL purge, and the
// hot-retention drop leaves the parquet_local row in place. The parquet itself
// is already durable in S3 and still backs hot reads, so it is not moved to
// upload-failed/; only upload_failed_at is set so the №2 slow re-test
// (RequeueQuarantinedParquet) retries the manifest on the same rate-limited
// cadence as a rejected parquet. Segment refcounts stay pinned meanwhile.
// first_failed_at is stamped once and never reset (see MarkUploadFailed), so a
// manifest that never heals ages out of the quarantine cap instead of pinning
// the PV forever behind an ever-refreshed upload_failed_at.
func (m *metaDb) QuarantineForManifest(path string, failedAtMs int64) error {
	return errors.Wrap(m.meta.Exec(`UPDATE parquet_local
		SET upload_failed_at = ?, first_failed_at = COALESCE(first_failed_at, ?)
		WHERE path = ? AND uploaded_at IS NULL`, failedAtMs, failedAtMs, path).Error,
		"quarantine sealed parquet for a rejected manifest")
}

// ManifestBounds reports min(time_min_ms) / max(time_max_ms) over one
// pod-restart's sealed files of one UTC day — the pods-manifest range
// (01-write-contract.md §3.6). ok is false when the day holds no sealed file.
func (m *metaDb) ManifestBounds(podRestart string, dayStartMs, dayEndMs int64) (timeMinMs, timeMaxMs int64, ok bool, err error) {
	var row struct {
		TimeMinMs *int64
		TimeMaxMs *int64
	}
	err = m.meta.Raw(`SELECT MIN(time_min_ms) AS time_min_ms, MAX(time_max_ms) AS time_max_ms
		FROM parquet_local WHERE pod_restart = ? AND time_bucket_ms >= ? AND time_bucket_ms < ?`,
		podRestart, dayStartMs, dayEndMs).Scan(&row).Error
	if err != nil || row.TimeMinMs == nil || row.TimeMaxMs == nil {
		return 0, 0, false, err
	}
	return *row.TimeMinMs, *row.TimeMaxMs, true, nil
}

// DeletableSegmentCandidates lists segments with no un-uploaded sealed rows
// left (refcount 0) in closed pod-restarts. The caller still has to prove the
// pod-restart holds no un-sealed calls before unlinking (03-lifecycle.md §3.7
// step 14): a refcount-0 segment may simply not be sealed yet.
func (m *metaDb) DeletableSegmentCandidates() ([]SegmentRow, error) {
	var rows []SegmentRow
	err := m.meta.Raw(`SELECT s.pod_restart, s.stream, s.rolling_seq, s.path, s.logical_size,
		s.time_min_ms, s.time_max_ms, s.refcount, s.status
		FROM segments s JOIN pod_restarts p ON p.pod_restart = s.pod_restart
		WHERE s.refcount = 0 AND s.status <> 'open' AND p.closed_at IS NOT NULL
		ORDER BY s.pod_restart, s.stream, s.rolling_seq`).Scan(&rows).Error
	return rows, err
}

// HasUnsealedCalls reports whether any bucket still holds indexed calls of the
// pod-restart past its seal watermark — rows a future seal will pin segments
// for, so the segments cannot be deleted yet.
func (m *metaDb) HasUnsealedCalls(podRestart string) (bool, error) {
	buckets, err := m.Buckets()
	if err != nil {
		return false, err
	}
	for _, bucket := range buckets {
		var maxOffset *int64
		if err := m.withPartition(bucket, func(db *gorm.DB) error {
			return db.Raw(`SELECT MAX(calls_wal_offset) FROM call_index WHERE pod_restart = ?`,
				podRestart).Scan(&maxOffset).Error
		}); err != nil {
			return false, err
		}
		if maxOffset == nil {
			continue
		}
		watermark, err := m.SealWatermark(podRestart, bucket)
		if err != nil {
			return false, err
		}
		if *maxOffset >= watermark {
			return true, nil
		}
	}
	return false, nil
}

// PurgeCallsPastWalEnd deletes the pod-restart's index rows whose
// calls_wal_offset lies at or past the WAL's valid end (№8): after a power
// loss the SQLite index can run ahead of a truncated calls.wal, and such a row
// would poison every seal of its bucket — loadSealRows can never find its
// record. The rows' data is gone with the torn WAL tail; dropping them is the
// 03 §4 "degrade, not fail" choice. Returns how many rows were dropped.
func (m *metaDb) PurgeCallsPastWalEnd(podRestart string, walEnd int64) (int64, error) {
	buckets, err := m.Buckets()
	if err != nil {
		return 0, err
	}
	var purged int64
	for _, bucket := range buckets {
		var affected int64
		if err := m.withPartition(bucket, func(db *gorm.DB) error {
			res := db.Exec(`DELETE FROM call_index WHERE pod_restart = ? AND calls_wal_offset >= ?`,
				podRestart, walEnd)
			if res.Error != nil {
				return errors.Wrapf(res.Error, "purge poisoned index rows of bucket %d", bucket)
			}
			affected = res.RowsAffected
			return nil
		}); err != nil {
			return purged, err
		}
		purged += affected
	}
	return purged, nil
}

// UnsealedPodRestarts lists every pod-restart that still holds indexed calls
// past its seal watermark, in one sweep over the partitions — the bulk form of
// HasUnsealedCalls the disk-budget eviction uses to keep segments a future
// seal will read out of the eviction order (№7).
func (m *metaDb) UnsealedPodRestarts() (map[string]bool, error) {
	buckets, err := m.Buckets()
	if err != nil {
		return nil, err
	}
	out := map[string]bool{}
	for _, bucket := range buckets {
		maxOffsets, err := m.MaxWalOffsets(bucket)
		if err != nil {
			return nil, err
		}
		for podRestart, maxOffset := range maxOffsets {
			if out[podRestart] {
				continue
			}
			watermark, err := m.SealWatermark(podRestart, bucket)
			if err != nil {
				return nil, err
			}
			if maxOffset >= watermark {
				out[podRestart] = true
			}
		}
	}
	return out, nil
}

// LivePodRestarts lists pod-restarts whose connection is still open
// (closed_at IS NULL). Their segments may be referenced by data that has not
// arrived yet — a PARAM_BIG_DEDUP tag pointing at a value sent hours ago, or
// a long call whose chunks predate its Call record — so the disk budget
// evicts them last (re-review finding 5).
func (m *metaDb) LivePodRestarts() (map[string]bool, error) {
	var rows []string
	err := m.meta.Raw(`SELECT pod_restart FROM pod_restarts WHERE closed_at IS NULL`).Scan(&rows).Error
	out := make(map[string]bool, len(rows))
	for _, r := range rows {
		out[r] = true
	}
	return out, err
}

// SegmentRefcount re-reads one segment's current refcount and status; the
// disk-budget eviction re-checks its candidates against this right before the
// unlink, because its candidate list may predate a seal commit that pinned the
// segment (№7).
func (m *metaDb) SegmentRefcount(podRestart, stream string, seq int) (refcount int, status string, err error) {
	var row struct {
		Refcount int
		Status   string
	}
	err = m.meta.Raw(`SELECT refcount, status FROM segments
		WHERE pod_restart = ? AND stream = ? AND rolling_seq = ?`,
		podRestart, stream, seq).Scan(&row).Error
	return row.Refcount, row.Status, err
}

// DeleteSegment removes one segment's catalog row; the refcount guard keeps a
// racing seal (which pins under its own transaction) safe.
func (m *metaDb) DeleteSegment(podRestart, stream string, seq int) error {
	return errors.Wrap(m.meta.Exec(`DELETE FROM segments
		WHERE pod_restart = ? AND stream = ? AND rolling_seq = ? AND refcount = 0`,
		podRestart, stream, seq).Error, "delete segment row")
}

// ParquetLocalPaths lists every sealed file the catalog believes exists.
func (m *metaDb) ParquetLocalPaths() ([]string, error) {
	var paths []string
	err := m.meta.Raw(`SELECT path FROM parquet_local ORDER BY path`).Scan(&paths).Error
	return paths, err
}

// MaxWalOffsets reports, per pod-restart, the highest calls.wal offset indexed
// in the bucket's partition; the seal loop compares it with the watermark.
func (m *metaDb) MaxWalOffsets(bucket int64) (map[string]int64, error) {
	var out map[string]int64
	err := m.withPartition(bucket, func(db *gorm.DB) error {
		var rows []struct {
			PodRestart string
			MaxOffset  int64
		}
		if err := db.Raw(`SELECT pod_restart, MAX(calls_wal_offset) AS max_offset
			FROM call_index GROUP BY pod_restart`).Scan(&rows).Error; err != nil {
			return err
		}
		out = make(map[string]int64, len(rows))
		for _, r := range rows {
			out[r.PodRestart] = r.MaxOffset
		}
		return nil
	})
	return out, err
}

// CallsForSeal reads the bucket's unsealed rows of one pod-restart in the
// §5.2 row order: (ts_ms DESC, pk ASC).
func (m *metaDb) CallsForSeal(bucket int64, podRestart string, watermark int64) ([]CallIndexRow, error) {
	var rows []CallIndexRow
	err := m.withPartition(bucket, func(db *gorm.DB) error {
		return db.Raw(`SELECT pod_restart, trace_file_index, buffer_offset, record_index,
			ts_ms, duration_ms, method_id, thread_name, retention_class, error_flag,
			cpu_time_ms, wait_time_ms, memory_used, child_calls, params_json, calls_wal_offset
			FROM call_index WHERE pod_restart = ? AND calls_wal_offset >= ?
			ORDER BY ts_ms DESC, trace_file_index ASC, buffer_offset ASC, record_index ASC`,
			podRestart, watermark).Scan(&rows).Error
	})
	return rows, err
}

// partitionPath renders <data>/calls-<bucketStart>.sqlite (01 §8).
func (m *metaDb) partitionPath(bucket int64) string {
	stamp := time.UnixMilli(m.cfg.BucketStartMs(bucket)).UTC().Format("20060102T150405Z")
	return filepath.Join(m.cfg.DataDir, fmt.Sprintf("calls-%s.sqlite", stamp))
}

// withPartition borrows the bucket's partition handle for the duration of fn.
// The handle is ref-counted while borrowed, so a concurrent LRU eviction (or a
// dropCachedPartition on another worker's error) defers its Close until the
// last borrower returns — a worker mid-query never races into "sql: database is
// closed" (PR 708 review #4). fn runs without m.mu held, so it may itself
// borrow other partitions (SealWatermark does) without deadlocking.
func (m *metaDb) withPartition(bucket int64, fn func(*gorm.DB) error) error {
	h, err := m.acquirePartition(bucket)
	if err != nil {
		return err
	}
	defer m.releasePartition(h)
	return fn(h.db)
}

// acquirePartition opens (creating on first use) the call-index partition of
// bucket and returns a borrowed handle the caller must releasePartition.
// Re-opening a dropped bucket resurrects it (dropped_at cleared): the only path
// that reaches a dropped bucket is InsertCall with a very late Call, and its
// row must re-enter the seal loop rather than land in an invisible file.
func (m *metaDb) acquirePartition(bucket int64) (*partHandle, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.useTick++
	if h, ok := m.parts[bucket]; ok {
		h.useTick = m.useTick
		h.refs++
		return h, nil
	}
	m.evictLocked()
	db, err := m.openPartition(bucket)
	if err != nil {
		return nil, err
	}
	h := &partHandle{db: db, useTick: m.useTick, refs: 1}
	m.parts[bucket] = h
	return h, nil
}

// releasePartition drops one borrow; a handle evicted while borrowed closes
// here, when its last borrower leaves.
func (m *metaDb) releasePartition(h *partHandle) {
	m.mu.Lock()
	defer m.mu.Unlock()
	h.refs--
	if h.refs == 0 && h.evicted {
		closeHandle(h)
	}
}

// evictLocked closes least-recently-used handles until the cache is back under
// PartitionCacheSize (№24). A borrowed victim is unlinked and flagged rather
// than closed; its Close waits for the last releasePartition. Unborrowed
// handles are preferred as victims so the common path frees a descriptor at
// once instead of leaving an evicted-but-open handle behind. Callers hold m.mu.
func (m *metaDb) evictLocked() {
	for len(m.parts) >= m.cfg.PartitionCacheSize {
		var victimKey int64
		var victim *partHandle
		for b, h := range m.parts {
			if victim == nil || betterVictim(h, victim) {
				victimKey, victim = b, h
			}
		}
		if victim == nil {
			return
		}
		delete(m.parts, victimKey)
		if victim.refs == 0 {
			closeHandle(victim)
		} else {
			victim.evicted = true
		}
	}
}

// betterVictim reports whether a is a better eviction victim than b: an
// unborrowed handle beats a borrowed one, and among equals the
// least-recently-used wins.
func betterVictim(a, b *partHandle) bool {
	if (a.refs == 0) != (b.refs == 0) {
		return a.refs == 0
	}
	return a.useTick < b.useTick
}

func closeHandle(h *partHandle) {
	if sqlDb, err := h.db.DB(); err == nil {
		_ = sqlDb.Close()
	}
}

// openPartition opens, connection-caps, and migrates the bucket's partition
// file, then records it in the catalog. Callers hold m.mu.
func (m *metaDb) openPartition(bucket int64) (*gorm.DB, error) {
	path := m.partitionPath(bucket)
	db, err := openSqlite(path)
	if err != nil {
		return nil, err
	}
	// One writer plus one reader saturates a per-bucket SQLite file; an
	// unbounded pool across cached buckets held hundreds of file handles (№24).
	if sqlDb, err := db.DB(); err == nil {
		sqlDb.SetMaxOpenConns(2)
		sqlDb.SetMaxIdleConns(2)
	}
	if err := db.Exec(partitionSchema).Error; err != nil {
		return nil, errors.Wrapf(err, "migrate partition %s", path)
	}
	// Metric columns that joined call_index after it shipped: a partition file
	// written by a pre-upgrade collector needs the ALTERs; a fresh file gets
	// the columns via CREATE TABLE, so the duplicate-column error is the
	// common case. Partitions outlive an upgrade by at most the hot window.
	for _, column := range []string{
		"queue_wait_ms", "suspend_ms", "transactions", "logs_generated",
		"logs_written", "file_read", "file_written", "net_read", "net_written",
	} {
		alter := fmt.Sprintf("ALTER TABLE call_index ADD COLUMN %s INTEGER NOT NULL DEFAULT 0", column)
		if err := db.Exec(alter).Error; err != nil &&
			!strings.Contains(err.Error(), "duplicate column name") {
			return nil, errors.Wrapf(err, "migrate partition %s: %s", path, alter)
		}
	}
	// method_text is nullable TEXT, so it cannot ride the INTEGER loop above.
	if err := db.Exec("ALTER TABLE call_index ADD COLUMN method_text TEXT").Error; err != nil &&
		!strings.Contains(err.Error(), "duplicate column name") {
		return nil, errors.Wrapf(err, "migrate partition %s: add method_text", path)
	}
	if err := m.meta.Exec(`INSERT OR IGNORE INTO call_partitions (bucket, path, created_at)
		VALUES (?, ?, ?)`, bucket, path, time.Now().UnixMilli()).Error; err != nil {
		return nil, errors.Wrap(err, "record partition")
	}
	if err := m.meta.Exec(`UPDATE call_partitions SET dropped_at = NULL
		WHERE bucket = ? AND dropped_at IS NOT NULL`, bucket).Error; err != nil {
		return nil, errors.Wrap(err, "resurrect partition")
	}
	return db, nil
}

// dropCachedPartition forgets one bucket's cached handle after an operation on
// it errored (InsertCall's retry path). A borrowed handle is flagged and closes
// on its last release, exactly like an LRU eviction.
func (m *metaDb) dropCachedPartition(bucket int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if h, ok := m.parts[bucket]; ok {
		delete(m.parts, bucket)
		if h.refs == 0 {
			closeHandle(h)
		} else {
			h.evicted = true
		}
	}
}

// InsertCall indexes one Call record in its bucket's partition. INSERT OR
// IGNORE keeps the recovery reconciliation (03 §3.4) idempotent: the PK is
// immutable, so a duplicate insert carries identical values. One retry with a
// fresh handle covers the race with a concurrent janitor DropPartition: the
// reopen resurrects the partition, so the late row stays visible.
func (m *metaDb) InsertCall(bucket int64, row CallIndexRow) error {
	insert := func() error {
		return m.withPartition(bucket, func(db *gorm.DB) error {
			return db.Exec(`INSERT OR IGNORE INTO call_index
				(pod_restart, trace_file_index, buffer_offset, record_index,
				 ts_ms, duration_ms, method_id, thread_name, retention_class, error_flag,
				 cpu_time_ms, wait_time_ms, memory_used, queue_wait_ms, suspend_ms, child_calls,
				 transactions, logs_generated, logs_written, file_read, file_written,
				 net_read, net_written, params_json, calls_wal_offset)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				row.PodRestart, row.TraceFileIndex, row.BufferOffset, row.RecordIndex,
				row.TsMs, row.DurationMs, row.MethodId, row.ThreadName, row.RetentionClass, row.ErrorFlag,
				row.CpuTimeMs, row.WaitTimeMs, row.MemoryUsed, row.QueueWaitMs, row.SuspendMs, row.ChildCalls,
				row.Transactions, row.LogsGenerated, row.LogsWritten, row.FileRead, row.FileWritten,
				row.NetRead, row.NetWritten, row.ParamsJson, row.CallsWalOffset).Error
		})
	}
	if err := insert(); err != nil {
		m.dropCachedPartition(bucket)
		return insert()
	}
	return nil
}

// MinTsMs reports one partition's earliest ts_ms, or nil when it is empty.
func (m *metaDb) MinTsMs(bucket int64) (*int64, error) {
	var min *int64
	err := m.withPartition(bucket, func(db *gorm.DB) error {
		return db.Raw(`SELECT MIN(ts_ms) FROM call_index`).Scan(&min).Error
	})
	return min, err
}

// callsQueryWhere renders the SQL-pushable predicates of q (№15): the ts
// window, the duration bounds, the error flag, the retention classes, and
// the pod filter (pod_restart is "<pod_id>/<restartMs>", so a pod matches by
// prefix). The method filter stays in Go — it matches the
// dictionary-resolved name, which SQL does not have.
func callsQueryWhere(q model.CallsQuery, fromMs, toMs int64) (string, []any) {
	where := "ts_ms >= ? AND ts_ms < ?"
	args := []any{fromMs, toMs}
	if q.DurationMinMs > 0 {
		where += " AND duration_ms >= ?"
		args = append(args, q.DurationMinMs)
	}
	if q.DurationMaxMs > 0 {
		where += " AND duration_ms <= ?"
		args = append(args, q.DurationMaxMs)
	}
	if q.ErrorOnly {
		where += " AND error_flag = 1"
	}
	if len(q.RetentionClasses) > 0 {
		where += " AND retention_class IN (?" + strings.Repeat(",?", len(q.RetentionClasses)-1) + ")"
		for _, c := range q.RetentionClasses {
			args = append(args, c)
		}
	}
	if len(q.Pods) > 0 {
		parts := make([]string, len(q.Pods))
		for i, p := range q.Pods {
			parts[i] = `pod_restart LIKE ? ESCAPE '\'`
			args = append(args, escapeLikePrefix(p)+"/%")
		}
		where += " AND (" + strings.Join(parts, " OR ") + ")"
	}
	return where, args
}

// escapeLikePrefix escapes the LIKE metacharacters in a literal prefix.
// Kubernetes names cannot carry them, but the query text is caller input.
func escapeLikePrefix(s string) string {
	r := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
	return r.Replace(s)
}

// CallsPage reads one partition's rows for the hot /calls path (№15): the
// filters push into SQL and the result is bounded to the top `limit` ts
// values plus the complete tie group at the boundary, newest first — so the
// caller can finish the (ts_ms DESC, pk ASC) order with the shared
// comparator and page further down with toMs = the last row's ts_ms. Fewer
// than `limit` rows back means the window is exhausted.
func (m *metaDb) CallsPage(bucket int64, q model.CallsQuery, fromMs, toMs int64, limit int) ([]CallIndexRow, error) {
	where, args := callsQueryWhere(q, fromMs, toMs)
	// The subquery finds the ts of the limit-th newest matching row; the outer
	// SELECT takes everything at or above it, so a tie group is never split
	// across pages (splitting one would break the keyset order at the cut).
	sql := `SELECT pod_restart, trace_file_index, buffer_offset, record_index,
		ts_ms, duration_ms, method_id, thread_name, retention_class, error_flag,
		cpu_time_ms, wait_time_ms, memory_used, queue_wait_ms, suspend_ms, child_calls,
		transactions, logs_generated, logs_written, file_read, file_written,
		net_read, net_written, params_json, calls_wal_offset, method_text
		FROM call_index WHERE ` + where + `
		AND ts_ms >= COALESCE((SELECT MIN(ts_ms) FROM (
			SELECT ts_ms FROM call_index WHERE ` + where + ` ORDER BY ts_ms DESC LIMIT ?)), ?)
		ORDER BY ts_ms DESC`
	allArgs := make([]any, 0, 2*len(args)+2)
	allArgs = append(allArgs, args...)
	allArgs = append(allArgs, args...)
	allArgs = append(allArgs, limit, fromMs)
	var rows []CallIndexRow
	err := m.withPartition(bucket, func(db *gorm.DB) error {
		return db.Raw(sql, allArgs...).Scan(&rows).Error
	})
	return rows, err
}

// FindCall probes one partition for a PK; the point SELECT rides the
// partition's primary key.
func (m *metaDb) FindCall(bucket int64, podRestart string, traceFileIndex, bufferOffset, recordIndex int) (CallIndexRow, bool, error) {
	var rows []CallIndexRow
	err := m.withPartition(bucket, func(db *gorm.DB) error {
		return db.Raw(`SELECT pod_restart, trace_file_index, buffer_offset, record_index,
			ts_ms, duration_ms, method_id, thread_name, retention_class, error_flag,
			cpu_time_ms, wait_time_ms, memory_used, queue_wait_ms, suspend_ms, child_calls,
			transactions, logs_generated, logs_written, file_read, file_written,
			net_read, net_written, params_json, calls_wal_offset, method_text
			FROM call_index WHERE pod_restart = ? AND trace_file_index = ? AND buffer_offset = ? AND record_index = ?`,
			podRestart, traceFileIndex, bufferOffset, recordIndex).Scan(&rows).Error
	})
	if err != nil || len(rows) == 0 {
		return CallIndexRow{}, false, err
	}
	return rows[0], true, nil
}

// PodWindows reports one partition's per-pod-restart [min, max] ts_ms bounds.
func (m *metaDb) PodWindows(bucket int64) (map[string][2]int64, error) {
	var out map[string][2]int64
	err := m.withPartition(bucket, func(db *gorm.DB) error {
		var rows []struct {
			PodRestart string
			TsMin      int64
			TsMax      int64
		}
		if err := db.Raw(`SELECT pod_restart, MIN(ts_ms) AS ts_min, MAX(ts_ms) AS ts_max
			FROM call_index GROUP BY pod_restart`).Scan(&rows).Error; err != nil {
			return err
		}
		out = make(map[string][2]int64, len(rows))
		for _, r := range rows {
			out[r.PodRestart] = [2]int64{r.TsMin, r.TsMax}
		}
		return nil
	})
	return out, err
}

// Buckets lists the known call-index partitions.
func (m *metaDb) Buckets() ([]int64, error) {
	var buckets []int64
	err := m.meta.Raw(`SELECT bucket FROM call_partitions WHERE dropped_at IS NULL ORDER BY bucket`).Scan(&buckets).Error
	return buckets, err
}

// Calls reads a bucket's call_index rows ordered by the primary time axis.
func (m *metaDb) Calls(bucket int64) ([]CallIndexRow, error) {
	var rows []CallIndexRow
	err := m.withPartition(bucket, func(db *gorm.DB) error {
		return db.Raw(`SELECT pod_restart, trace_file_index, buffer_offset, record_index,
			ts_ms, duration_ms, method_id, thread_name, retention_class, error_flag,
			cpu_time_ms, wait_time_ms, memory_used, child_calls, params_json, calls_wal_offset,
			method_text
			FROM call_index ORDER BY ts_ms, pod_restart`).Scan(&rows).Error
	})
	return rows, err
}

// AgedUploadedParquet lists uploaded files past hot retention:
// uploaded_at + retention <= now (01-write-contract.md §6.3). Quarantined and
// pending files have uploaded_at NULL and never age out.
func (m *metaDb) AgedUploadedParquet(nowMs, retentionMs int64) ([]ParquetLocalFile, error) {
	var rows []ParquetLocalFile
	err := m.meta.Raw(`SELECT path, pod_restart, time_bucket_ms, retention_class, seq, row_count,
		time_min_ms, time_max_ms, file_size, uploaded_at AS uploaded_at_ms,
		upload_failed_at AS upload_failed_at_ms, s3_key
		FROM parquet_local WHERE uploaded_at IS NOT NULL AND uploaded_at + ? <= ?
		ORDER BY path`, retentionMs, nowMs).Scan(&rows).Error
	return rows, err
}

// DeleteParquetLocalRow forgets one uploaded file the hot-retention janitor
// deleted. Unlike DropParquetLocal it releases nothing: MarkUploaded already
// released the segment refcounts when the upload committed.
func (m *metaDb) DeleteParquetLocalRow(path string) error {
	return errors.Wrap(m.meta.Exec(`DELETE FROM parquet_local WHERE path = ?`, path).Error,
		"delete aged parquet row")
}

// BucketParquetCount reports how many parquet_local rows still reference the
// bucket, in any state. A bucket is droppable from the hot index only at zero:
// a pending or quarantined row means rows not yet durable in S3, and an
// uploaded row still inside hot_retention means the overlap window is open.
func (m *metaDb) BucketParquetCount(timeBucketMs int64) (int, error) {
	var n int
	err := m.meta.Raw(`SELECT COUNT(*) FROM parquet_local WHERE time_bucket_ms = ?`,
		timeBucketMs).Scan(&n).Error
	return n, err
}

// DropPartition closes the bucket's partition handle and marks the catalog row
// dropped; the caller unlinks the SQLite files. Buckets() stops listing the
// bucket, so every Buckets-driven reader (hot window, seal loop, FindCall)
// forgets it atomically with the catalog update.
func (m *metaDb) DropPartition(bucket, droppedAtMs int64) (string, error) {
	m.dropCachedPartition(bucket)
	var path string
	if err := m.meta.Raw(`SELECT path FROM call_partitions WHERE bucket = ?`, bucket).
		Scan(&path).Error; err != nil {
		return "", err
	}
	return path, errors.Wrap(m.meta.Exec(`UPDATE call_partitions SET dropped_at = ?
		WHERE bucket = ? AND dropped_at IS NULL`, droppedAtMs, bucket).Error, "mark partition dropped")
}

// WalPurgeCandidate is a closed pod-restart whose WALs are still on the PV.
type WalPurgeCandidate struct {
	PodRestart string
	ClosedAtMs int64
}

// WalPurgeCandidates lists pod-restarts eligible for the 03 §3.9 step-18 WAL
// purge check: closed and not yet purged. There is no snapshot gate any
// more — sealed rows are self-contained (№3) — so the caller's remaining
// checks (every sealed file uploaded, nothing indexed in the hot tier, the
// hold-back grace) are the whole condition.
func (m *metaDb) WalPurgeCandidates() ([]WalPurgeCandidate, error) {
	var rows []WalPurgeCandidate
	err := m.meta.Raw(`SELECT pod_restart, closed_at AS closed_at_ms
		FROM pod_restarts
		WHERE closed_at IS NOT NULL AND wals_purged_at IS NULL
		ORDER BY pod_restart`).Scan(&rows).Error
	return rows, err
}

// BackfillMethodText materializes resolved method names into the pod-restart's
// call_index rows before the fast-path WAL purge deletes the dictionary they
// resolve against. Only NULL rows are touched, so a crashed purge re-runs as a
// no-op even after the dictionary is gone. Returns how many rows still lack a
// name (an id the resolver missed); the caller degrades those to the "#<id>"
// placeholder the read path already renders for an unknown word.
func (m *metaDb) BackfillMethodText(podRestart string, resolve func(id int) (string, bool)) (unresolved int64, err error) {
	buckets, err := m.Buckets()
	if err != nil {
		return 0, err
	}
	for _, bucket := range buckets {
		if err := m.withPartition(bucket, func(db *gorm.DB) error {
			var ids []int
			if err := db.Raw(`SELECT DISTINCT method_id FROM call_index
				WHERE pod_restart = ? AND method_text IS NULL`, podRestart).Scan(&ids).Error; err != nil {
				return err
			}
			for _, id := range ids {
				word, ok := resolve(id)
				if !ok {
					var n int64
					if err := db.Raw(`SELECT COUNT(*) FROM call_index
						WHERE pod_restart = ? AND method_id = ? AND method_text IS NULL`,
						podRestart, id).Scan(&n).Error; err != nil {
						return err
					}
					unresolved += n
					continue
				}
				if err := db.Exec(`UPDATE call_index SET method_text = ?
					WHERE pod_restart = ? AND method_id = ? AND method_text IS NULL`,
					word, podRestart, id).Error; err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return unresolved, errors.Wrapf(err, "backfill method_text of %s in bucket %d", podRestart, bucket)
		}
	}
	return unresolved, nil
}

// HasPendingParquet reports whether any of the pod-restart's sealed files is
// not confirmed in S3 (pending upload or quarantined) — either blocks the WAL
// purge: the WAL is the only source a re-seal could decode the rows from.
func (m *metaDb) HasPendingParquet(podRestart string) (bool, error) {
	var n int
	err := m.meta.Raw(`SELECT COUNT(*) FROM parquet_local
		WHERE pod_restart = ? AND uploaded_at IS NULL`, podRestart).Scan(&n).Error
	return n > 0, err
}

// SetWalsPurged records that the pod-restart's WAL files are gone.
func (m *metaDb) SetWalsPurged(podRestart string, purgedAtMs int64) error {
	return errors.Wrap(m.meta.Exec(`UPDATE pod_restarts SET wals_purged_at = ?
		WHERE pod_restart = ? AND wals_purged_at IS NULL`, purgedAtMs, podRestart).Error,
		"set wals_purged_at")
}

// QuarantineStats aggregates the stuck-quarantine state for the metrics
// endpoint: rejected parquet files (parquet_local.upload_failed_at, 01 §8).
// Oldest is the earliest FIRST-failure timestamp (first_failed_at, immutable
// across re-tests), nil when nothing is quarantined — so the age gauge climbs
// with the true stuck duration instead of plateauing at the re-test interval.
type QuarantineStats struct {
	ParquetCount    int64
	ParquetOldestMs *int64
}

// QuarantineStats reports how much quarantined state waits for resolution.
// The population shrinks on manual intervention, on a successful №2 re-test,
// or on the №2 cap (dropped parquet) — a sustained non-zero count is still
// the alerting signal.
func (m *metaDb) QuarantineStats() (QuarantineStats, error) {
	var out QuarantineStats
	row := struct {
		N      int64
		Oldest *int64
	}{}
	if err := m.meta.Raw(`SELECT COUNT(*) AS n, MIN(COALESCE(first_failed_at, upload_failed_at)) AS oldest
		FROM parquet_local WHERE upload_failed_at IS NOT NULL`).Scan(&row).Error; err != nil {
		return out, errors.Wrap(err, "count quarantined parquet")
	}
	out.ParquetCount, out.ParquetOldestMs = row.N, row.Oldest
	return out, nil
}

// EvictedSegmentKeys lists the (pod_restart, stream, rolling_seq) of every
// evicted segment; the janitor joins them against the in-RAM chunk index to
// measure the dangling-refs gauge.
func (m *metaDb) EvictedSegmentKeys() ([]SegmentRow, error) {
	var rows []SegmentRow
	err := m.meta.Raw(`SELECT pod_restart, stream, rolling_seq
		FROM segments WHERE status = 'evicted'`).Scan(&rows).Error
	return rows, err
}

// SegmentsForBudget lists every non-evicted segment in the deterministic
// eviction order: oldest first by created_at, tie-broken by the catalog key
// (01-write-contract.md §4.6 — refcount partitioning happens in the caller).
func (m *metaDb) SegmentsForBudget() ([]SegmentRow, error) {
	var rows []SegmentRow
	err := m.meta.Raw(`SELECT pod_restart, stream, rolling_seq, path, logical_size,
		time_min_ms, time_max_ms, refcount, status
		FROM segments WHERE status <> 'evicted'
		ORDER BY created_at, pod_restart, stream, rolling_seq`).Scan(&rows).Error
	return rows, err
}

// MarkSegmentEvicted records a disk-budget eviction: the file is gone but the
// row survives with status 'evicted' and its refcount untouched, so the seal
// pass truncates the affected calls with disk_budget (01-write-contract.md
// §4.6) and an upload can still release the refcounts it pinned.
func (m *metaDb) MarkSegmentEvicted(podRestart, stream string, seq int, evictedAtMs int64) error {
	return errors.Wrap(m.meta.Exec(`UPDATE segments SET status = 'evicted', evicted_at = ?
		WHERE pod_restart = ? AND stream = ? AND rolling_seq = ?`,
		evictedAtMs, podRestart, stream, seq).Error, "mark segment evicted")
}

// MaxCallsWalOffset reports the highest calls.wal offset indexed for a
// pod-restart across all partitions, or ok=false when none is indexed. The
// recovery reconciliation re-inserts every calls.wal record past it (03 §3.4).
func (m *metaDb) MaxCallsWalOffset(podRestart string) (offset int64, ok bool, err error) {
	buckets, err := m.Buckets()
	if err != nil {
		return 0, false, err
	}
	for _, bucket := range buckets {
		var max *int64
		if err := m.withPartition(bucket, func(db *gorm.DB) error {
			return db.Raw(`SELECT MAX(calls_wal_offset) FROM call_index WHERE pod_restart = ?`,
				podRestart).Scan(&max).Error
		}); err != nil {
			return 0, false, err
		}
		if max != nil && (!ok || *max > offset) {
			offset, ok = *max, true
		}
	}
	return offset, ok, nil
}
