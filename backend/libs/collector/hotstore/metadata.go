package hotstore

import (
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

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
  dict_uploaded_at INTEGER
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
  uploaded_at     INTEGER,
  upload_failed_at INTEGER,
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
  child_calls      INTEGER NOT NULL DEFAULT 0,
  params_json      TEXT,
  calls_wal_offset INTEGER NOT NULL,
  blob_size        INTEGER,
  truncated_reason TEXT,
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
		ChildCalls     int
		ParamsJson     string
		CallsWalOffset int64
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
		S3Key          string
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
		S3Key            string
	}

	// metaDb wraps metadata.sqlite plus the per-bucket partition handles.
	metaDb struct {
		cfg  Config
		meta *gorm.DB

		mu    sync.Mutex
		parts map[int64]*gorm.DB
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
	// upload_failed_at joined the schema with the S3 slice; a metadata.sqlite
	// created before it needs the column added (new files get it via CREATE
	// TABLE above, so the duplicate-column error is the common case).
	if err := db.Exec(`ALTER TABLE parquet_local ADD COLUMN upload_failed_at INTEGER`).Error; err != nil &&
		!strings.Contains(err.Error(), "duplicate column name") {
		return nil, errors.Wrap(err, "add parquet_local.upload_failed_at")
	}
	return &metaDb{cfg: cfg, meta: db, parts: map[int64]*gorm.DB{}}, nil
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
	for _, p := range m.parts {
		closeGorm(p)
	}
	m.parts = map[int64]*gorm.DB{}
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
// (01-write-contract.md §6.2, 03-lifecycle.md §3.2).
func (m *metaDb) RecordSealedFile(row parquetLocalRow, segRows map[segKey]int) error {
	return m.meta.Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec(`INSERT INTO parquet_local
			(path, pod_restart, time_bucket_ms, retention_class, seq, row_count,
			 time_min_ms, time_max_ms, file_size, sealed_at, uploaded_at, s3_key)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?)`,
			row.Path, row.PodRestart, row.TimeBucketMs, row.RetentionClass, row.Seq, row.RowCount,
			row.TimeMinMs, row.TimeMaxMs, row.FileSize, row.SealedAtMs, row.S3Key).Error; err != nil {
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
	})
}

// LocalParquet lists a pod-restart's sealed files still held locally.
func (m *metaDb) LocalParquet(podRestart string) ([]ParquetLocalFile, error) {
	var rows []ParquetLocalFile
	err := m.meta.Raw(`SELECT path, pod_restart, time_bucket_ms, retention_class, seq, row_count,
		time_min_ms, time_max_ms, file_size, uploaded_at AS uploaded_at_ms,
		upload_failed_at AS upload_failed_at_ms, s3_key
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
// pinned; used when the file vanished before its upload (03-lifecycle.md §3.6).
func (m *metaDb) DropParquetLocal(path string) error {
	return m.meta.Transaction(func(tx *gorm.DB) error {
		if err := releaseSealedFileRefs(tx, path); err != nil {
			return err
		}
		return errors.Wrap(tx.Exec(`DELETE FROM parquet_local WHERE path = ?`, path).Error,
			"drop sealed parquet row")
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
// and takes it out of the upload queue. The parquet_segments rows stay: a
// rejected file keeps its segments pinned until a human resolves it (01 §8).
func (m *metaDb) MarkUploadFailed(path, quarantinePath string, failedAtMs int64) error {
	return errors.Wrap(m.meta.Exec(`UPDATE parquet_local SET path = ?, upload_failed_at = ?
		WHERE path = ? AND uploaded_at IS NULL`, quarantinePath, failedAtMs, path).Error,
		"mark sealed parquet upload-failed")
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

// DictPendingPodRestarts lists closed pod-restarts whose dictionary/suspend
// snapshots are still owed to S3 (03-lifecycle.md §3.9).
func (m *metaDb) DictPendingPodRestarts() ([]string, error) {
	var rows []string
	err := m.meta.Raw(`SELECT pod_restart FROM pod_restarts
		WHERE closed_at IS NOT NULL AND dict_uploaded_at IS NULL ORDER BY pod_restart`).Scan(&rows).Error
	return rows, err
}

// SetDictUploaded gates the snapshot upload (01-write-contract.md §3.6): set
// only after both the dictionary and suspend objects are in S3.
func (m *metaDb) SetDictUploaded(podRestart string, uploadedAtMs int64) error {
	return errors.Wrap(m.meta.Exec(`UPDATE pod_restarts SET dict_uploaded_at = ?
		WHERE pod_restart = ? AND dict_uploaded_at IS NULL`, uploadedAtMs, podRestart).Error,
		"set dict_uploaded_at")
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
		db, err := m.partition(bucket)
		if err != nil {
			return false, err
		}
		var maxOffset *int64
		if err := db.Raw(`SELECT MAX(calls_wal_offset) FROM call_index WHERE pod_restart = ?`,
			podRestart).Scan(&maxOffset).Error; err != nil {
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
	db, err := m.partition(bucket)
	if err != nil {
		return nil, err
	}
	var rows []struct {
		PodRestart string
		MaxOffset  int64
	}
	if err := db.Raw(`SELECT pod_restart, MAX(calls_wal_offset) AS max_offset
		FROM call_index GROUP BY pod_restart`).Scan(&rows).Error; err != nil {
		return nil, err
	}
	out := make(map[string]int64, len(rows))
	for _, r := range rows {
		out[r.PodRestart] = r.MaxOffset
	}
	return out, nil
}

// CallsForSeal reads the bucket's unsealed rows of one pod-restart in the
// §5.2 row order: (ts_ms DESC, pk ASC).
func (m *metaDb) CallsForSeal(bucket int64, podRestart string, watermark int64) ([]CallIndexRow, error) {
	db, err := m.partition(bucket)
	if err != nil {
		return nil, err
	}
	var rows []CallIndexRow
	err = db.Raw(`SELECT pod_restart, trace_file_index, buffer_offset, record_index,
		ts_ms, duration_ms, method_id, thread_name, retention_class, error_flag,
		cpu_time_ms, wait_time_ms, memory_used, child_calls, params_json, calls_wal_offset
		FROM call_index WHERE pod_restart = ? AND calls_wal_offset >= ?
		ORDER BY ts_ms DESC, trace_file_index ASC, buffer_offset ASC, record_index ASC`,
		podRestart, watermark).Scan(&rows).Error
	return rows, err
}

// partitionPath renders <data>/calls-<bucketStart>.sqlite (01 §8).
func (m *metaDb) partitionPath(bucket int64) string {
	stamp := time.UnixMilli(m.cfg.BucketStartMs(bucket)).UTC().Format("20060102T150405Z")
	return filepath.Join(m.cfg.DataDir, fmt.Sprintf("calls-%s.sqlite", stamp))
}

// partition opens (creating on first use) the call-index partition of bucket.
func (m *metaDb) partition(bucket int64) (*gorm.DB, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if db, ok := m.parts[bucket]; ok {
		return db, nil
	}
	path := m.partitionPath(bucket)
	db, err := openSqlite(path)
	if err != nil {
		return nil, err
	}
	if err := db.Exec(partitionSchema).Error; err != nil {
		return nil, errors.Wrapf(err, "migrate partition %s", path)
	}
	if err := m.meta.Exec(`INSERT OR IGNORE INTO call_partitions (bucket, path, created_at)
		VALUES (?, ?, ?)`, bucket, path, time.Now().UnixMilli()).Error; err != nil {
		return nil, errors.Wrap(err, "record partition")
	}
	m.parts[bucket] = db
	return db, nil
}

// InsertCall indexes one Call record in its bucket's partition. INSERT OR
// IGNORE keeps the recovery reconciliation (03 §3.4) idempotent: the PK is
// immutable, so a duplicate insert carries identical values.
func (m *metaDb) InsertCall(bucket int64, row CallIndexRow) error {
	db, err := m.partition(bucket)
	if err != nil {
		return err
	}
	return db.Exec(`INSERT OR IGNORE INTO call_index
		(pod_restart, trace_file_index, buffer_offset, record_index,
		 ts_ms, duration_ms, method_id, thread_name, retention_class, error_flag,
		 cpu_time_ms, wait_time_ms, memory_used, child_calls, params_json, calls_wal_offset)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		row.PodRestart, row.TraceFileIndex, row.BufferOffset, row.RecordIndex,
		row.TsMs, row.DurationMs, row.MethodId, row.ThreadName, row.RetentionClass, row.ErrorFlag,
		row.CpuTimeMs, row.WaitTimeMs, row.MemoryUsed, row.ChildCalls, row.ParamsJson, row.CallsWalOffset).Error
}

// Buckets lists the known call-index partitions.
func (m *metaDb) Buckets() ([]int64, error) {
	var buckets []int64
	err := m.meta.Raw(`SELECT bucket FROM call_partitions WHERE dropped_at IS NULL ORDER BY bucket`).Scan(&buckets).Error
	return buckets, err
}

// Calls reads a bucket's call_index rows ordered by the primary time axis.
func (m *metaDb) Calls(bucket int64) ([]CallIndexRow, error) {
	db, err := m.partition(bucket)
	if err != nil {
		return nil, err
	}
	var rows []CallIndexRow
	err = db.Raw(`SELECT pod_restart, trace_file_index, buffer_offset, record_index,
		ts_ms, duration_ms, method_id, thread_name, retention_class, error_flag,
		cpu_time_ms, wait_time_ms, memory_used, child_calls, params_json, calls_wal_offset
		FROM call_index ORDER BY ts_ms, pod_restart`).Scan(&rows).Error
	return rows, err
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
		db, err := m.partition(bucket)
		if err != nil {
			return 0, false, err
		}
		var max *int64
		if err := db.Raw(`SELECT MAX(calls_wal_offset) FROM call_index WHERE pod_restart = ?`,
			podRestart).Scan(&max).Error; err != nil {
			return 0, false, err
		}
		if max != nil && (!ok || *max > offset) {
			offset, ok = *max, true
		}
	}
	return offset, ok, nil
}
