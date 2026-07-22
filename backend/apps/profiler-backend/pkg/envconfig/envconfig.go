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

	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	"github.com/Netcracker/qubership-profiler-backend/libs/s3"
	"github.com/kelseyhightower/envconfig"
	"github.com/pkg/errors"
)

type (
	// Collect is the `collect` subcommand configuration.
	Collect struct {
		LogLevel string `envconfig:"PROFILER_LOG_LEVEL" default:"info"`

		DataDir           string        `envconfig:"PROFILER_DATA_DIR" default:"/data"`
		AgentPort         int           `envconfig:"PROFILER_AGENT_PORT" default:"1715"`
		InternalAPIPort   int           `envconfig:"PROFILER_INTERNAL_API_PORT" default:"8081"`
		TimeBucket        time.Duration `envconfig:"PROFILER_TIME_BUCKET" default:"5m"`
		TimeBucketGrace   time.Duration `envconfig:"PROFILER_TIME_BUCKET_GRACE" default:"30s"`
		DictFsyncRecords  int           `envconfig:"PROFILER_DICT_FSYNC_RECORDS" default:"256"`
		DictFsyncInterval time.Duration `envconfig:"PROFILER_DICT_FSYNC_INTERVAL" default:"100ms"`
		// DurationThresholds override the clean-tier bounds of the shared
		// retention tier table (01 §6.4). Unset keeps the table defaults
		// (100ms,1s,10s) — the default deliberately lives in ONE place, the
		// model.RetentionTiers table, not in a tag here (№10).
		DurationThresholds  DurationThresholds `envconfig:"PROFILER_DURATION_THRESHOLDS"`
		SegmentRotationSize ByteSize           `envconfig:"PROFILER_SEGMENT_ROTATION_SIZE" default:"4MB"`

		// Replica names this instance in sealed-file names and S3 keys
		// (01 §7). The StatefulSet passes its pod name (04 §3.2); outside k8s
		// the wiring falls back to HOSTNAME, then to the libs default.
		Replica string `envconfig:"STATEFULSET_ORDINAL"`

		// SealCheckInterval / UploadCheckInterval / JanitorCheckInterval pace
		// the background loops. The contract defines the triggers (01 §6.1,
		// §6.3) but not the poll cadence, so these names are an implementation
		// choice.
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
		// implementation choice.
		WalPurgeGrace time.Duration `envconfig:"PROFILER_WAL_PURGE_GRACE" default:"1h"`
		// WalPurgeFastMaxBytes is the near-empty floor of the fast-path WAL
		// purge (03 §3.9): a closed pod-restart at or under it purges after the
		// grace without waiting for its call-index partition drop. 0 disables
		// the fast path.
		WalPurgeFastMaxBytes ByteSize `envconfig:"PROFILER_WAL_PURGE_FAST_MAX_BYTES" default:"16MiB"`
		// MemBudget caps the hot store's in-RAM pod-restart state (01 §9,
		// §4.6): over budget the janitor unloads closed pod-restarts'
		// dictionaries and, once fully sealed, their chunk indexes.
		MemBudget ByteSize `envconfig:"PROFILER_MEM_BUDGET" default:"2GB"`
		// PendingUploadMaxBytes bounds the un-uploaded backlog (sealed parquet
		// owed to S3, live call partitions, and the tracked pod-restarts' WAL
		// files) when S3 falls behind: once pending parquet alone reaches half
		// the budget sealing pauses, once the whole backlog reaches the full
		// budget ingest refuses agent data with ACK_ERROR so the PV never runs
		// to ENOSPC. The agent drops the refused window and reconnects (01
		// §4.6) — the loss is counted on ingest_refused_bytes_total. The env
		// name is an implementation choice, like the quarantine knobs below.
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

		// PprofEnabled mounts net/http/pprof on the internal API port for
		// load tests and incident debugging (load-testing-plan.md §6).
		// Default off: profiles cost CPU when taken and have no place in
		// routine operation. The env name is an implementation choice.
		PprofEnabled bool `envconfig:"PROFILER_PPROF_ENABLED"`

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
		PodsRangeLimit   time.Duration `envconfig:"PROFILER_MAX_PODS_RANGE" default:"8784h"`
		MaxScanFiles     int           `envconfig:"PROFILER_MAX_SCAN_FILES" default:"10000"`
		MaxScanBytes     ByteSize      `envconfig:"PROFILER_MAX_SCAN_BYTES" default:"2GB"`
		// ReadMemoryBudget / ReadBudgetWait shape the process-wide read
		// memory budget and its admission queue (02 §7.5).
		ReadMemoryBudget ByteSize      `envconfig:"PROFILER_READ_MEMORY_BUDGET" default:"512MB"`
		ReadBudgetWait   time.Duration `envconfig:"PROFILER_READ_BUDGET_WAIT" default:"5s"`
		// DurationThresholds must mirror the collector's value: the cold
		// class pruning and the guard exemption derive their bounds from the
		// same tier table the seal pass classified with (№10). Unset keeps
		// the model.RetentionTiers defaults.
		DurationThresholds DurationThresholds `envconfig:"PROFILER_DURATION_THRESHOLDS"`

		ShutdownDrainGrace time.Duration `envconfig:"PROFILER_SHUTDOWN_DRAIN_GRACE" default:"30s"`

		// PprofEnabled mounts net/http/pprof on the external API port —
		// query has no internal port (04 §12), so the profiles ride the
		// same listener as /api/v1. The ingress publishes /api/v1 only, so
		// /debug/pprof stays cluster-internal; still, default off.
		PprofEnabled bool `envconfig:"PROFILER_PPROF_ENABLED"`

		// DumpsCollectorURL is the dumps-collector base URL, e.g.
		// "https://dumps-collector-<namespace>.<cloud-public-host>" — a
		// separate deployment with its own ingress, so there is no in-cluster
		// way to derive it. Empty leaves the Pods Info dump link-out
		// unavailable (PR 708 review #18).
		DumpsCollectorURL string `envconfig:"DUMPS_COLLECTOR_URL"`

		S3 S3
	}

	// Maintain is the `maintain` subcommand configuration (01 §9, 03 §10):
	// the compaction knobs of 01 §6.6 and the per-class retention TTLs of
	// 01 §6.4.
	Maintain struct {
		LogLevel string `envconfig:"PROFILER_LOG_LEVEL" default:"info"`

		// CheckInterval paces the maintenance loop, mirroring the collector's
		// *_CHECK_INTERVAL knobs; the name is an implementation choice.
		CheckInterval time.Duration `envconfig:"PROFILER_MAINTAIN_CHECK_INTERVAL" default:"5m"`
		// MetricsPort serves /metrics and /health/live in loop mode; the
		// one-shot --run-now mode (a CronJob pod) binds nothing.
		MetricsPort int `envconfig:"PROFILER_METRICS_PORT" default:"8081"`
		// TimeBucket must mirror the collector's value: the settled check
		// needs the bucket end and the object key carries only the start.
		TimeBucket time.Duration `envconfig:"PROFILER_TIME_BUCKET" default:"5m"`

		// PprofEnabled mounts net/http/pprof on the metrics port in loop
		// mode; the one-shot --run-now mode binds nothing. Default off.
		PprofEnabled bool `envconfig:"PROFILER_PPROF_ENABLED"`

		CompactionMinAge      time.Duration `envconfig:"PROFILER_COMPACTION_MIN_AGE" default:"30m"`
		CompactionMinFiles    int           `envconfig:"PROFILER_COMPACTION_MIN_FILES" default:"4"`
		CompactionDeleteGrace time.Duration `envconfig:"PROFILER_COMPACTION_DELETE_GRACE" default:"5m"`
		CompactionMaxBytes    ByteSize      `envconfig:"PROFILER_COMPACTION_MAX_BYTES" default:"256MB"`

		// Per-class retention TTLs (01 §6.4). Unset values keep the defaults
		// of the model.RetentionTiers table — the defaults deliberately live
		// in ONE place, not in tags here (№10). RetentionPodsTTL expires the
		// pods/v1 manifests (01 §3.6); its default derives from the longest
		// class TTL plus a margin.
		RetentionShortCleanTTL  TTL `envconfig:"PROFILER_RETENTION_SHORT_CLEAN_TTL"`
		RetentionNormalCleanTTL TTL `envconfig:"PROFILER_RETENTION_NORMAL_CLEAN_TTL"`
		RetentionLongCleanTTL   TTL `envconfig:"PROFILER_RETENTION_LONG_CLEAN_TTL"`
		RetentionHugeCleanTTL   TTL `envconfig:"PROFILER_RETENTION_HUGE_CLEAN_TTL"`
		RetentionAnyErrorTTL    TTL `envconfig:"PROFILER_RETENTION_ANY_ERROR_TTL"`
		RetentionCorruptedTTL   TTL `envconfig:"PROFILER_RETENTION_CORRUPTED_TTL"`
		RetentionPodsTTL        TTL `envconfig:"PROFILER_RETENTION_PODS_TTL"`

		S3 S3
	}

	// S3 carries the object-store connection shared by the subcommands
	// (01 §9). The scheme of S3_ENDPOINT selects TLS. S3_PATH_PREFIX roots
	// EVERY object key (parquet and the pods manifests) below the bucket, so
	// several deployments can share one bucket with per-deployment prefixes
	// (01 §7, 04 §6); empty keeps the keys at the bucket root. Each
	// credential comes from exactly one source: the env value (dev, compose)
	// or a file path (k8s mounts the Secret as a volume, 04 §6). CAFile and
	// InsecureSSL only apply over https:// endpoints; libs/s3.Params.IsValid
	// rejects them otherwise. CAFile trusts a private CA in addition to the
	// system store. InsecureSSL is a dev/smoke escape hatch that skips
	// verification entirely — never set it in production.
	S3 struct {
		Endpoint      string `envconfig:"S3_ENDPOINT" required:"true"`
		Bucket        string `envconfig:"S3_BUCKET" required:"true"`
		PathPrefix    string `envconfig:"S3_PATH_PREFIX"`
		AccessKey     string `envconfig:"S3_ACCESS_KEY"`
		SecretKey     string `envconfig:"S3_SECRET_KEY"`
		AccessKeyFile string `envconfig:"S3_ACCESS_KEY_FILE"`
		SecretKeyFile string `envconfig:"S3_SECRET_KEY_FILE"`
		CAFile        string `envconfig:"S3_CA_FILE"`
		InsecureSSL   bool   `envconfig:"S3_INSECURE_SKIP_VERIFY"`
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
		CAFile:          s.CAFile,
		InsecureSSL:     s.InsecureSSL,
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
	if err := envconfig.Process("", &c); err != nil {
		return c, errors.Wrap(err, "parse collect env")
	}
	return c, validateTimeBucket(c.TimeBucket)
}

// ParseQuery reads the `query` configuration from the environment.
func ParseQuery() (Query, error) {
	var q Query
	if err := envconfig.Process("", &q); err != nil {
		return q, errors.Wrap(err, "parse query env")
	}
	// A negative wait would silently fall back to the library default inside
	// query.Config.Normalize; a misconfiguration must fail loudly instead.
	if q.ReadBudgetWait < 0 {
		return q, errors.Errorf("PROFILER_READ_BUDGET_WAIT must not be negative, got %s", q.ReadBudgetWait)
	}
	return q, nil
}

// ParseMaintain reads the `maintain` configuration from the environment.
func ParseMaintain() (Maintain, error) {
	var m Maintain
	if err := envconfig.Process("", &m); err != nil {
		return m, errors.Wrap(err, "parse maintain env")
	}
	return m, validateTimeBucket(m.TimeBucket)
}

// validateTimeBucket rejects a PROFILER_TIME_BUCKET that does not divide the
// hour (№28): rows are filed under the hour prefix of their bucket start and
// cold discovery walks whole hours (02 §5.1) on the premise that a bucket
// never spans an hour boundary — a 7-minute bucket would seal files no hour
// walk ever lists, making them invisible to cold reads.
func validateTimeBucket(bucket time.Duration) error {
	if bucket <= 0 || bucket > time.Hour || time.Hour%bucket != 0 {
		return errors.Errorf("PROFILER_TIME_BUCKET %s must divide the hour (e.g. 1m, 5m, 15m, 1h)", bucket)
	}
	return nil
}

// ClassTTLs resolves the per-class retention TTLs: explicit env values win,
// unset classes keep the tier-table defaults (№10).
func (m Maintain) ClassTTLs() map[string]time.Duration {
	out := model.DefaultClassTTL()
	for class, ttl := range map[string]TTL{
		model.RetentionShortClean:  m.RetentionShortCleanTTL,
		model.RetentionNormalClean: m.RetentionNormalCleanTTL,
		model.RetentionLongClean:   m.RetentionLongCleanTTL,
		model.RetentionHugeClean:   m.RetentionHugeCleanTTL,
		model.RetentionAnyError:    m.RetentionAnyErrorTTL,
		model.RetentionCorrupted:   m.RetentionCorruptedTTL,
	} {
		if ttl > 0 {
			out[class] = time.Duration(ttl)
		}
	}
	return out
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

// DurationThresholds decodes PROFILER_DURATION_THRESHOLDS: the ascending
// clean-tier boundaries, one per finite bound of the model.RetentionTiers
// table — "100ms,1s,10s" with the default table (01 §6.4). Nil (the env
// unset) keeps the table defaults downstream.
type DurationThresholds []time.Duration

// Decode implements envconfig.Decoder.
func (d *DurationThresholds) Decode(value string) error {
	parts := strings.Split(value, ",")
	want := len(model.CleanTiers()) - 1
	if len(parts) != want {
		return errors.Errorf("duration thresholds %q: want %d comma-separated durations, one per finite tier bound", value, want)
	}
	out := make([]time.Duration, len(parts))
	for i, part := range parts {
		v, err := time.ParseDuration(strings.TrimSpace(part))
		if err != nil {
			return errors.Wrapf(err, "duration thresholds %q", value)
		}
		if v <= 0 || (i > 0 && v <= out[i-1]) {
			return errors.Errorf("duration thresholds %q: want ascending positive durations", value)
		}
		out[i] = v
	}
	*d = out
	return nil
}
