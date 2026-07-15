// Package envconfig parses the profiler-backend configuration from the
// environment, following the catalogues of 01-write-contract.md §9,
// 02-read-contract.md §9, and 03-lifecycle.md §10. Only the knobs the
// composed services actually honour are parsed: accepting an env var the
// process would ignore misleads operators, so knobs of unshipped features
// stay unparsed until those features land.
package envconfig

import (
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/s3"
	"github.com/kelseyhightower/envconfig"
	"github.com/pkg/errors"
)

type (
	// Collect is the `collect` subcommand configuration.
	Collect struct {
		LogLevel string `envconfig:"PROFILER_LOG_LEVEL" default:"info"`

		DataDir             string             `envconfig:"PROFILER_DATA_DIR" default:"/data"`
		AgentPort           int                `envconfig:"PROFILER_AGENT_PORT" default:"1715"`
		InternalAPIPort     int                `envconfig:"PROFILER_INTERNAL_API_PORT" default:"8081"`
		TimeBucket          time.Duration      `envconfig:"PROFILER_TIME_BUCKET" default:"5m"`
		TimeBucketGrace     time.Duration      `envconfig:"PROFILER_TIME_BUCKET_GRACE" default:"30s"`
		DictFsyncRecords    int                `envconfig:"PROFILER_DICT_FSYNC_RECORDS" default:"256"`
		DictFsyncInterval   time.Duration      `envconfig:"PROFILER_DICT_FSYNC_INTERVAL" default:"100ms"`
		DurationThresholds  DurationThresholds `envconfig:"PROFILER_DURATION_THRESHOLDS" default:"100ms,1s"`
		SegmentRotationSize ByteSize           `envconfig:"PROFILER_SEGMENT_ROTATION_SIZE" default:"4MB"`

		// Replica names this instance in sealed-file names and S3 keys
		// (01 §7). The StatefulSet passes its pod name (04 §3.2); outside k8s
		// the wiring falls back to HOSTNAME, then to the libs default.
		Replica string `envconfig:"STATEFULSET_ORDINAL"`

		// SealCheckInterval / UploadCheckInterval / JanitorCheckInterval pace
		// the background loops. The contract defines the triggers (01 §6.1,
		// §6.3) but not the poll cadence, so these names are an implementation
		// choice recorded in stage1-progress.md.
		SealCheckInterval    time.Duration `envconfig:"PROFILER_SEAL_CHECK_INTERVAL" default:"15s"`
		UploadCheckInterval  time.Duration `envconfig:"PROFILER_UPLOAD_CHECK_INTERVAL" default:"30s"`
		JanitorCheckInterval time.Duration `envconfig:"PROFILER_JANITOR_CHECK_INTERVAL" default:"30s"`
		// SealConcurrency bounds the parallel seal passes of one loop tick
		// (01 §6.1, §9).
		SealConcurrency int `envconfig:"PROFILER_SEAL_CONCURRENCY" default:"4"`

		// HotRetention keeps uploaded parquet and the matching call-index
		// partitions on the PV past upload (01 §6.3, 02 §4.2).
		HotRetention time.Duration `envconfig:"PROFILER_HOT_RETENTION" default:"15m"`
		// ChunksStagingMaxBytes bounds the hot-store segment files on disk;
		// over budget the janitor evicts per 01 §4.6.
		ChunksStagingMaxBytes ByteSize `envconfig:"PROFILER_CHUNKS_STAGING_MAX_BYTES" default:"10GB"`
		// WalPurgeGrace is the 01 §3.5 hold-back before a fully flushed
		// pod-restart's WAL files are deleted; the env name is an
		// implementation choice recorded in stage1-progress.md.
		WalPurgeGrace time.Duration `envconfig:"PROFILER_WAL_PURGE_GRACE" default:"1h"`
		// MemBudget caps the hot store's in-RAM pod-restart state (01 §9,
		// §4.6): over budget the janitor unloads closed pod-restarts'
		// dictionaries and, once fully sealed, their chunk indexes.
		MemBudget ByteSize `envconfig:"PROFILER_MEM_BUDGET" default:"2GB"`
		// PendingUploadMaxBytes bounds the un-uploaded backlog (sealed parquet
		// owed to S3 plus live call partitions) when S3 falls behind: once
		// pending parquet alone reaches half the budget sealing pauses, once
		// the whole backlog reaches the full budget ingest refuses agent data
		// so the PV never runs to ENOSPC. The env name is an implementation
		// choice recorded in stage1-progress.md, like the quarantine knobs
		// below.
		PendingUploadMaxBytes ByteSize `envconfig:"PROFILER_PENDING_UPLOAD_MAX_BYTES" default:"2GB"`
		// QuarantineRetestInterval re-tests permanently-rejected uploads;
		// QuarantineMaxAge / QuarantineMaxBytes cap the upload-failed/
		// quarantine so it cannot pin the WAL purge or fill the PV.
		QuarantineRetestInterval time.Duration `envconfig:"PROFILER_QUARANTINE_RETEST_INTERVAL" default:"1h"`
		QuarantineMaxAge         TTL           `envconfig:"PROFILER_QUARANTINE_MAX_AGE" default:"7d"`
		QuarantineMaxBytes       ByteSize      `envconfig:"PROFILER_QUARANTINE_MAX_BYTES" default:"1GB"`
		// UploadConcurrency bounds the parallel S3 PUT workers of one upload
		// pass.
		UploadConcurrency int `envconfig:"PROFILER_UPLOAD_CONCURRENCY" default:"4"`

		ShutdownDrainGrace time.Duration `envconfig:"PROFILER_SHUTDOWN_DRAIN_GRACE" default:"30s"`

		S3 S3
	}

	// Query is the `query` subcommand configuration (02 §9).
	Query struct {
		LogLevel string `envconfig:"PROFILER_LOG_LEVEL" default:"info"`

		ExternalAPIPort  int           `envconfig:"PROFILER_EXTERNAL_API_PORT" default:"8080"`
		CollectorService string        `envconfig:"COLLECTOR_HEADLESS_SVC"`
		CollectorPort    int           `envconfig:"PROFILER_INTERNAL_API_PORT" default:"8081"`
		OverlapMargin    time.Duration `envconfig:"PROFILER_OVERLAP_MARGIN" default:"5m"`
		FanoutTimeout    time.Duration `envconfig:"PROFILER_FANOUT_TIMEOUT" default:"2s"`
		ListConcurrency  int           `envconfig:"PROFILER_S3_LIST_CONCURRENCY" default:"16"`
		CursorTTL        time.Duration `envconfig:"PROFILER_CURSOR_TTL" default:"15m"`
		WideRangeLimit   time.Duration `envconfig:"PROFILER_WIDE_RANGE_LIMIT" default:"6h"`
		MaxScanFiles     int           `envconfig:"PROFILER_MAX_SCAN_FILES" default:"10000"`
		MaxScanBytes     ByteSize      `envconfig:"PROFILER_MAX_SCAN_BYTES" default:"2GB"`

		ShutdownDrainGrace time.Duration `envconfig:"PROFILER_SHUTDOWN_DRAIN_GRACE" default:"30s"`

		S3 S3
	}

	// Maintain is the `maintain` subcommand configuration (01 §9, 03 §10):
	// the compaction knobs of 01 §6.6 and the per-class retention TTLs of
	// 01 §6.4.
	Maintain struct {
		LogLevel string `envconfig:"PROFILER_LOG_LEVEL" default:"info"`

		// CheckInterval paces the maintenance loop, mirroring the collector's
		// *_CHECK_INTERVAL knobs; the name is an implementation choice
		// recorded in stage1-progress.md.
		CheckInterval time.Duration `envconfig:"PROFILER_MAINTAIN_CHECK_INTERVAL" default:"5m"`
		// MetricsPort serves /metrics and /health/live in loop mode; the
		// one-shot --run-now mode (a CronJob pod) binds nothing.
		MetricsPort int `envconfig:"PROFILER_METRICS_PORT" default:"8081"`
		// TimeBucket must mirror the collector's value: the settled check
		// needs the bucket end and the object key carries only the start.
		TimeBucket time.Duration `envconfig:"PROFILER_TIME_BUCKET" default:"5m"`

		CompactionMinAge      time.Duration `envconfig:"PROFILER_COMPACTION_MIN_AGE" default:"30m"`
		CompactionMinFiles    int           `envconfig:"PROFILER_COMPACTION_MIN_FILES" default:"4"`
		CompactionDeleteGrace time.Duration `envconfig:"PROFILER_COMPACTION_DELETE_GRACE" default:"5m"`
		CompactionMaxBytes    ByteSize      `envconfig:"PROFILER_COMPACTION_MAX_BYTES" default:"256MB"`

		RetentionShortCleanTTL  TTL `envconfig:"PROFILER_RETENTION_SHORT_CLEAN_TTL" default:"1d"`
		RetentionNormalCleanTTL TTL `envconfig:"PROFILER_RETENTION_NORMAL_CLEAN_TTL" default:"7d"`
		RetentionLongCleanTTL   TTL `envconfig:"PROFILER_RETENTION_LONG_CLEAN_TTL" default:"30d"`
		RetentionAnyErrorTTL    TTL `envconfig:"PROFILER_RETENTION_ANY_ERROR_TTL" default:"30d"`
		RetentionCorruptedTTL   TTL `envconfig:"PROFILER_RETENTION_CORRUPTED_TTL" default:"7d"`
		RetentionDictionaryTTL  TTL `envconfig:"PROFILER_RETENTION_DICTIONARY_TTL" default:"35d"`

		S3 S3
	}

	// S3 carries the object-store connection shared by the subcommands
	// (01 §9). The scheme of S3_ENDPOINT selects TLS; the path prefix is not
	// configurable — the seal pass bakes `parquet/v1` into every key (01 §7).
	// Each credential comes from exactly one source: the env value (dev,
	// compose) or a file path (k8s mounts the Secret as a volume, 04 §6).
	S3 struct {
		Endpoint      string `envconfig:"S3_ENDPOINT" required:"true"`
		Bucket        string `envconfig:"S3_BUCKET" required:"true"`
		AccessKey     string `envconfig:"S3_ACCESS_KEY"`
		SecretKey     string `envconfig:"S3_SECRET_KEY"`
		AccessKeyFile string `envconfig:"S3_ACCESS_KEY_FILE"`
		SecretKeyFile string `envconfig:"S3_SECRET_KEY_FILE"`
	}
)

// Params maps the env shape onto the libs/s3 connection parameters, resolving
// file-based credentials. Files are read once, at startup; picking up a
// rotated Secret without a restart is a recorded open issue.
func (s S3) Params() (s3.Params, error) {
	access, err := credential("S3_ACCESS_KEY", s.AccessKey, s.AccessKeyFile)
	if err != nil {
		return s3.Params{}, err
	}
	secret, err := credential("S3_SECRET_KEY", s.SecretKey, s.SecretKeyFile)
	if err != nil {
		return s3.Params{}, err
	}
	p := s3.Params{
		Endpoint:        s.Endpoint,
		AccessKeyID:     access,
		SecretAccessKey: secret,
		UseSSL:          strings.HasPrefix(s.Endpoint, "https://"),
		BucketName:      s.Bucket,
	}
	p.Prepare() // strips the scheme the UseSSL check just consumed
	return p, nil
}

// credential resolves one env-or-file credential pair. Trailing whitespace is
// trimmed from the file body: Secret files routinely carry a final newline
// that would otherwise become part of the key.
func credential(name, value, file string) (string, error) {
	switch {
	case value != "" && file != "":
		return "", errors.Errorf("%s and %s_FILE are both set; set exactly one", name, name)
	case file != "":
		body, err := os.ReadFile(file)
		if err != nil {
			return "", errors.Wrapf(err, "read %s_FILE", name)
		}
		v := strings.TrimRight(string(body), " \t\r\n")
		if v == "" {
			return "", errors.Errorf("%s_FILE %q holds an empty credential", name, file)
		}
		return v, nil
	case value != "":
		return value, nil
	default:
		return "", errors.Errorf("missing %s: set it or %s_FILE", name, name)
	}
}

// ParseCollect reads the `collect` configuration from the environment.
func ParseCollect() (Collect, error) {
	var c Collect
	err := envconfig.Process("", &c)
	return c, errors.Wrap(err, "parse collect env")
}

// ParseQuery reads the `query` configuration from the environment.
func ParseQuery() (Query, error) {
	var q Query
	err := envconfig.Process("", &q)
	return q, errors.Wrap(err, "parse query env")
}

// ParseMaintain reads the `maintain` configuration from the environment.
func ParseMaintain() (Maintain, error) {
	var m Maintain
	err := envconfig.Process("", &m)
	return m, errors.Wrap(err, "parse maintain env")
}

// ByteSize decodes the contract's size literals ("64MB", "2GB", plain
// bytes). Suffixes are powers of 1024; the IEC spellings (KiB, MiB, ...) are
// accepted as synonyms.
type ByteSize int64

var byteSuffixes = []struct {
	suffix string
	shift  uint
}{
	{"TIB", 40}, {"TB", 40}, {"T", 40},
	{"GIB", 30}, {"GB", 30}, {"G", 30},
	{"MIB", 20}, {"MB", 20}, {"M", 20},
	{"KIB", 10}, {"KB", 10}, {"K", 10},
	{"B", 0},
}

// Decode implements envconfig.Decoder.
func (b *ByteSize) Decode(value string) error {
	raw := strings.ToUpper(strings.TrimSpace(value))
	shift := uint(0)
	for _, s := range byteSuffixes {
		if strings.HasSuffix(raw, s.suffix) {
			raw, shift = strings.TrimSpace(strings.TrimSuffix(raw, s.suffix)), s.shift
			break
		}
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n < 0 {
		return errors.Errorf("byte size %q: want <non-negative integer>[KB|MB|GB|TB]", value)
	}
	if shift > 0 && n > (1<<63-1)>>shift {
		return errors.Errorf("byte size %q overflows int64", value)
	}
	*b = ByteSize(n << shift)
	return nil
}

// TTL decodes the contract's retention literals (01 §9): a plain Go
// duration, or "<n>d" for n whole days ("1d", "35d") — time.ParseDuration
// has no day unit.
type TTL time.Duration

// Decode implements envconfig.Decoder.
func (t *TTL) Decode(value string) error {
	raw := strings.TrimSpace(value)
	if days, ok := strings.CutSuffix(raw, "d"); ok {
		n, err := strconv.Atoi(strings.TrimSpace(days))
		if err != nil || n < 0 {
			return errors.Errorf("ttl %q: want <non-negative integer>d or a Go duration", value)
		}
		*t = TTL(time.Duration(n) * 24 * time.Hour)
		return nil
	}
	v, err := time.ParseDuration(raw)
	if err != nil || v < 0 {
		return errors.Errorf("ttl %q: want <non-negative integer>d or a Go duration", value)
	}
	*t = TTL(v)
	return nil
}

// DurationThresholds decodes PROFILER_DURATION_THRESHOLDS: two ascending
// duration-class boundaries, "100ms,1s" (01 §6.4).
type DurationThresholds [2]time.Duration

// Decode implements envconfig.Decoder.
func (d *DurationThresholds) Decode(value string) error {
	parts := strings.Split(value, ",")
	if len(parts) != 2 {
		return errors.Errorf("duration thresholds %q: want two comma-separated durations", value)
	}
	for i, part := range parts {
		v, err := time.ParseDuration(strings.TrimSpace(part))
		if err != nil {
			return errors.Wrapf(err, "duration thresholds %q", value)
		}
		d[i] = v
	}
	if d[0] <= 0 || d[1] <= d[0] {
		return errors.Errorf("duration thresholds %q: want 0 < first < second", value)
	}
	return nil
}
