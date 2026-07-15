// Package hotstore persists the demultiplexed agent streams on the collector's
// local PV: append-only WALs for dictionary/params/suspend and the raw Call
// records, gzip segments for the offset-addressable bulk streams (trace, sql,
// xml), and the SQLite metadata that indexes them. It implements the write-path
// side of backend/docs/design/01-write-contract.md §3-§4, the recovery sequence
// of 03-lifecycle.md §3, the seal pass of 01 §5-§6 that materializes the CallV2
// parquet files locally, and the Uploader that makes them durable in S3 along
// with the per-pod-restart snapshot objects (01 §3.6, §6.2).
package hotstore

import "time"

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
	// (PROFILER_DURATION_THRESHOLDS, default 100ms,1s; see §6.4).
	DurationThresholds [2]time.Duration
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
	// implementation choice recorded in stage1-progress.md.
	WalPurgeGrace time.Duration
	// JanitorCheckInterval paces JanitorPass (hot retention, WAL purge, disk
	// budget). Zero disables the loop, mirroring SealCheckInterval: the collect
	// wiring enables it; tests drive JanitorPass explicitly.
	JanitorCheckInterval time.Duration
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
	if c.DurationThresholds[0] <= 0 {
		c.DurationThresholds[0] = 100 * time.Millisecond
	}
	if c.DurationThresholds[1] <= 0 {
		c.DurationThresholds[1] = time.Second
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
	return c
}

// Retention classes derived at write time from (duration, error_flag)
// (01-write-contract.md §6.4). The corrupted class is reserved and never
// populated in the MVP (§5.6).
const (
	RetentionShortClean  = "short_clean"
	RetentionNormalClean = "normal_clean"
	RetentionLongClean   = "long_clean"
	RetentionAnyError    = "any_error"
)

// RetentionClass classifies one call for parquet sharding and TTL.
func (c Config) RetentionClass(duration time.Duration, errorFlag bool) string {
	switch {
	case errorFlag:
		return RetentionAnyError
	case duration < c.DurationThresholds[0]:
		return RetentionShortClean
	case duration < c.DurationThresholds[1]:
		return RetentionNormalClean
	default:
		return RetentionLongClean
	}
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
