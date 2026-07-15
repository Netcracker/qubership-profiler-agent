// Package hotstore persists the demultiplexed agent streams on the collector's
// local PV: append-only WALs for dictionary/params/suspend and the raw Call
// records, gzip segments for the offset-addressable bulk streams (trace, sql,
// xml), and the SQLite metadata that indexes them. It implements the write-path
// side of backend/docs/design/01-write-contract.md §3-§4, the recovery sequence
// of 03-lifecycle.md §3, the seal pass of 01 §5-§6 that materializes the CallV2
// parquet files locally, and the Uploader that makes them durable in S3 along
// with the per-day pods/v1 identity manifests (01 §3.6, §6.2).
package hotstore

import (
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	"github.com/pkg/errors"
)

// Config carries the write-path knobs from 01-write-contract.md §9. Zero
// values fall back to the contract defaults via Normalize.
type Config struct {
	// DataDir is the PV root (PROFILER_DATA_DIR, default /data).
	DataDir string
	// TimeBucket is the call-index partition width (PROFILER_TIME_BUCKET).
	TimeBucket time.Duration
	// DictFsyncRecords / DictFsyncInterval bound the WAL fsync lag
	// (PROFILER_DICT_FSYNC_RECORDS / PROFILER_DICT_FSYNC_INTERVAL).
	DictFsyncRecords  int
	DictFsyncInterval time.Duration
	// DurationThresholds split clean calls into retention classes
	// (PROFILER_DURATION_THRESHOLDS, default 100ms,1s,10s; §6.4). One value
	// per finite clean-tier bound of the model.RetentionTiers table; nil
	// falls back to the table defaults.
	DurationThresholds []time.Duration
	// TimeBucketGrace is the wait past a bucket's end before it seals
	// (PROFILER_TIME_BUCKET_GRACE, default 30s; §6.1).
	TimeBucketGrace time.Duration
	// Replica names the producer in sealed-file names and S3 keys (§7,
	// STATEFULSET_ORDINAL). The collector app wiring will derive it from
	// HOSTNAME; the default keeps single-replica runs deterministic.
	Replica string
	// SealSpillBytes bounds one call's in-RAM blob during a seal pass; a
	// larger blob overflows to a temp file under parquet-sealing/ (§6.5).
	// Full-pass accounting arrives with PROFILER_MEM_BUDGET (budgets task).
	SealSpillBytes int64
	// SealCheckInterval paces the seal loop (§6.1). Zero disables the loop:
	// the collector app wiring enables it; tests seal explicitly.
	SealCheckInterval time.Duration
	// SealConcurrency bounds the seal passes one SealDue runs in parallel
	// (PROFILER_SEAL_CONCURRENCY, §6.1). Each (pod-restart, bucket) pair still
	// seals exactly once; the pool only widens across pairs.
	SealConcurrency int
	// UploadCheckInterval paces the S3 upload loop (§6.2, 03 §3.8). Zero
	// disables it, mirroring SealCheckInterval; tests drive Uploader.Pass.
	UploadCheckInterval time.Duration
	// UploadRetryAttempts / UploadRetryBaseDelay bound the in-pass exponential
	// backoff on a retryable S3 error (§6.2 step 4). A file that exhausts the
	// attempts stays pending and the next pass starts over, so the retry is
	// unbounded across passes as the contract requires.
	UploadRetryAttempts  int
	UploadRetryBaseDelay time.Duration
	// HotRetention keeps uploaded parquet and its call-index partition on the
	// PV past the upload (PROFILER_HOT_RETENTION, §6.3, 02 §4.2). Must satisfy
	// hot_retention >= seal_interval + overlap_margin for the zero-gap window.
	HotRetention time.Duration
	// ChunksStagingMaxBytes bounds the on-disk bytes of the hot-store segment
	// files (PROFILER_CHUNKS_STAGING_MAX_BYTES, §4.6). Over budget, the janitor
	// evicts refcount-0 segments first, then the oldest referenced ones.
	ChunksStagingMaxBytes int64
	// WalPurgeGrace is the hold-back past a pod-restart's full flush before its
	// WAL files are deleted (§3.5, 03 §3.9 step 18). The env name is an
	// implementation choice.
	WalPurgeGrace time.Duration
	// JanitorCheckInterval paces JanitorPass (hot retention, WAL purge, disk
	// budget). Zero disables the loop, mirroring SealCheckInterval: the collect
	// wiring enables it; tests drive JanitorPass explicitly.
	JanitorCheckInterval time.Duration
	// MemBudgetBytes caps the hot store's in-RAM pod-restart state
	// (PROFILER_MEM_BUDGET, §4.6). Over budget the janitor unloads closed
	// pod-restarts' dictionaries and, once fully sealed, their chunk indexes;
	// both reload or degrade explicitly (№1).
	MemBudgetBytes int64
	// PendingUploadMaxBytes bounds the un-uploaded backlog on the PV — sealed
	// parquet still owed to S3, the live call-index partitions, and the
	// tracked pod-restarts' WAL files — when S3 falls behind (№2, re-review
	// finding 4). Once the pending parquet alone reaches half the budget the
	// seal loop pauses (the data stays in WALs and segments); once the whole
	// backlog reaches the full budget ingest refuses RCV_DATA with ACK_ERROR
	// before writing. The agent does NOT buffer and retry on ACK_ERROR: it
	// treats it as fatal, drops the unacknowledged window, and reconnects as
	// a fresh pod-restart (06 §6) — a bounded, counted loss
	// (ingest_refused_bytes_total) instead of the PV running to ENOSPC.
	PendingUploadMaxBytes int64
	// QuarantineRetestInterval is how often a permanently-rejected upload is
	// re-tested (№2): "permanent" rejections are often operational (expired
	// credentials, missing bucket) and heal without a human.
	QuarantineRetestInterval time.Duration
	// QuarantineMaxAge / QuarantineMaxBytes cap the upload-failed/ quarantine
	// (№2). Past either cap the janitor drops the oldest quarantined parquet —
	// bounded, loudly-logged data loss — so a rejection can neither fill the
	// PV nor pin the WAL purge forever.
	QuarantineMaxAge   time.Duration
	QuarantineMaxBytes int64
	// UploadConcurrency bounds the parallel S3 PUT workers of one upload pass
	// (№25): a single slow PUT must not head-of-line-block the whole backlog.
	UploadConcurrency int
	// PartitionCacheSize caps the open per-bucket SQLite handles (№24); the
	// least-recently-used handle closes when a new bucket needs a slot.
	PartitionCacheSize int
}

// Normalize fills unset fields with the contract defaults.
func (c Config) Normalize() Config {
	if c.TimeBucket <= 0 {
		c.TimeBucket = 5 * time.Minute
	}
	if c.DictFsyncRecords <= 0 {
		c.DictFsyncRecords = 256
	}
	if c.DictFsyncInterval <= 0 {
		c.DictFsyncInterval = 100 * time.Millisecond
	}
	if len(c.DurationThresholds) == 0 {
		c.DurationThresholds = model.DefaultDurationThresholds()
	}
	if c.TimeBucketGrace <= 0 {
		c.TimeBucketGrace = 30 * time.Second
	}
	if c.Replica == "" {
		c.Replica = "collector-0"
	}
	if c.SealSpillBytes <= 0 {
		c.SealSpillBytes = 16 << 20
	}
	if c.SealConcurrency <= 0 {
		c.SealConcurrency = 4
	}
	if c.UploadRetryAttempts <= 0 {
		c.UploadRetryAttempts = 5
	}
	if c.UploadRetryBaseDelay <= 0 {
		c.UploadRetryBaseDelay = 200 * time.Millisecond
	}
	if c.HotRetention <= 0 {
		c.HotRetention = 15 * time.Minute
	}
	if c.ChunksStagingMaxBytes <= 0 {
		c.ChunksStagingMaxBytes = 10 << 30
	}
	if c.WalPurgeGrace <= 0 {
		c.WalPurgeGrace = time.Hour
	}
	if c.MemBudgetBytes <= 0 {
		c.MemBudgetBytes = 2 << 30
	}
	if c.PendingUploadMaxBytes <= 0 {
		c.PendingUploadMaxBytes = 2 << 30
	}
	if c.QuarantineRetestInterval <= 0 {
		c.QuarantineRetestInterval = time.Hour
	}
	if c.QuarantineMaxAge <= 0 {
		c.QuarantineMaxAge = 7 * 24 * time.Hour
	}
	if c.QuarantineMaxBytes <= 0 {
		c.QuarantineMaxBytes = 1 << 30
	}
	if c.UploadConcurrency <= 0 {
		c.UploadConcurrency = 4
	}
	if c.PartitionCacheSize <= 0 {
		c.PartitionCacheSize = 8
	}
	return c
}

// Retention classes derived at write time from (duration, error_flag)
// (01-write-contract.md §6.4). Aliases of the shared model constants so the
// write side, the read pruning, and maintenance all key off the one tier
// table (№10). The corrupted class is reserved and never populated in the
// MVP (§5.6).
const (
	RetentionShortClean  = model.RetentionShortClean
	RetentionNormalClean = model.RetentionNormalClean
	RetentionLongClean   = model.RetentionLongClean
	RetentionHugeClean   = model.RetentionHugeClean
	RetentionAnyError    = model.RetentionAnyError
)

// RetentionClass classifies one call for parquet sharding and TTL, walking
// the shared tier table with this deployment's thresholds.
func (c Config) RetentionClass(duration time.Duration, errorFlag bool) string {
	return model.ClassifyDuration(duration, errorFlag, c.DurationThresholds)
}

// Validate rejects a configuration the storage layout cannot serve. The time
// bucket must divide the hour: rows are filed under the hour prefix of their
// bucket start, and cold discovery walks whole hours (02 §5.1) on the
// premise that a bucket never spans an hour boundary — a 7-minute bucket
// would seal files that no hour walk ever lists (№28).
func (c Config) Validate() error {
	bucket := c.TimeBucket
	if bucket <= 0 {
		return nil // Normalize supplies the default
	}
	if bucket > time.Hour || time.Hour%bucket != 0 {
		return errors.Errorf("PROFILER_TIME_BUCKET %s must divide the hour (e.g. 1m, 5m, 15m, 1h)", bucket)
	}
	if want := len(model.CleanTiers()) - 1; len(c.DurationThresholds) != 0 && len(c.DurationThresholds) != want {
		return errors.Errorf("PROFILER_DURATION_THRESHOLDS needs exactly %d ascending values, got %d",
			want, len(c.DurationThresholds))
	}
	for i := 1; i < len(c.DurationThresholds); i++ {
		if c.DurationThresholds[i] <= c.DurationThresholds[i-1] {
			return errors.Errorf("PROFILER_DURATION_THRESHOLDS must ascend: %v", c.DurationThresholds)
		}
	}
	return nil
}

// Bucket maps a call start time to its partition index,
// floor(ts_ms / TimeBucket) (03-lifecycle.md §3.2).
func (c Config) Bucket(tsMs int64) int64 {
	return tsMs / c.TimeBucket.Milliseconds()
}

// BucketStartMs is the inverse of Bucket for naming partitions.
func (c Config) BucketStartMs(bucket int64) int64 {
	return bucket * c.TimeBucket.Milliseconds()
}
