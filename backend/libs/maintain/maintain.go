package maintain

import (
	"context"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
)

const (
	// producerToken is the reserved <replica> slot value of a compacted
	// object key (01-write-contract.md §7).
	producerToken = "maintain"
	// parquetPrefix roots every sealed-file key (01 §7).
	parquetPrefix = "parquet/v1"
)

type (
	// Config tunes one maintenance job. Zero fields take the contract
	// defaults via Normalize.
	Config struct {
		// TimeBucket mirrors the producer's PROFILER_TIME_BUCKET (01 §9): the
		// settled check needs the bucket end, and the key carries only the
		// start. A mismatch with the collector only shifts the settled point
		// by the difference.
		TimeBucket time.Duration
		// MinAge is how long past its end a bucket must be before compaction
		// (PROFILER_COMPACTION_MIN_AGE): a younger bucket may still receive
		// late-arrival patch files (01 §6.6), which would immediately
		// re-fragment the compacted output.
		MinAge time.Duration
		// MinFiles is the object count that triggers a fresh compaction of a
		// bucket-class group (PROFILER_COMPACTION_MIN_FILES). Residue of an
		// earlier round — a maintain object plus stragglers — recompacts
		// below the threshold so a bucket still converges to one object.
		MinFiles int
		// DeleteGrace delays the delete of a compaction's inputs after the
		// compacted object is written (PROFILER_COMPACTION_DELETE_GRACE,
		// 01 §6.6): a query whose LIST saw the inputs must still be able to
		// read them one discovery-plus-read round later.
		DeleteGrace time.Duration
		// MaxGroupBytes is the target ceiling for a compacted object: a group
		// over it is split into sub-budget objects by a streaming k-way merge
		// (one read-ahead batch per input, never the whole group in RAM), and
		// a single input already over it is left uncompacted, which readers
		// tolerate.
		MaxGroupBytes int64
		// ClassTTL maps a retention class to its TTL (01 §6.4). A class
		// absent from the map never expires.
		ClassTTL map[string]time.Duration
		// PodsManifestTTL expires the pods/v1 manifest objects
		// (PROFILER_RETENTION_PODS_TTL, 01 §3.6). It must exceed the longest
		// parquet class TTL so a readable row never outlives the manifest
		// naming its pod-restart.
		PodsManifestTTL time.Duration
	}

	// Stats reports what one Pass did; the counters back the future
	// Prometheus metrics and the test assertions.
	Stats struct {
		CompactedGroups     int // fresh compactions: one output written each
		CompactedInputFiles int // inputs consumed by those outputs
		CompactedRows       int // rows written into compacted objects
		DedupedRows         int // duplicate-PK rows dropped by the merge
		PendingDeleteGroups int // groups whose output exists, delete-grace pending
		DeletedInputFiles   int // inputs removed after the grace
		SkippedSmallGroups  int // groups below MinFiles with no residue
		SkippedUnsettled    int // groups younger than bucket end + MinAge
		SkippedOversized    int // single objects too big to join any sub-budget subgroup (№11)
		TTLParquetDeleted   int // parquet objects past their class TTL
		TTLManifestsDeleted int // pods/v1 manifests past PodsManifestTTL
		Errors              int // logged failures the pass skipped over
	}

	// Job runs maintenance passes against one object store.
	Job struct {
		store ObjectStore
		cfg   Config

		// OnPass, when set, receives every completed Pass's stats — the
		// Prometheus counter seam. RunLoop calls it from the loop goroutine
		// after each pass, including empty ones.
		OnPass func(Stats)
	}
)

// DefaultClassTTL returns the 01 §6.4 default per-class TTL mapping, derived
// from the shared tier table (№10) — never a local copy.
func DefaultClassTTL() map[string]time.Duration {
	return model.DefaultClassTTL()
}

// DefaultPodsManifestTTL derives the pods/v1 manifest retention from the
// tier table: the longest parquet class TTL plus a safety margin, so a
// readable row never outlives the manifest naming its pod-restart (01 §3.6).
func DefaultPodsManifestTTL() time.Duration {
	return model.MaxClassTTL() + 5*24*time.Hour
}

// isNoteworthy reports whether a pass changed state or hit an error, as
// opposed to routine skip counters (nothing settled or ready yet) that run
// every tick regardless (PR 708 review #27).
func (s Stats) isNoteworthy() bool {
	return s.CompactedGroups > 0 || s.DeletedInputFiles > 0 ||
		s.TTLParquetDeleted > 0 || s.TTLManifestsDeleted > 0 || s.Errors > 0
}

// Normalize fills unset fields with the contract defaults (01 §9).
func (c Config) Normalize() Config {
	if c.TimeBucket <= 0 {
		c.TimeBucket = 5 * time.Minute
	}
	if c.MinAge <= 0 {
		c.MinAge = 30 * time.Minute
	}
	if c.MinFiles <= 0 {
		c.MinFiles = 4
	}
	if c.DeleteGrace <= 0 {
		c.DeleteGrace = 5 * time.Minute
	}
	if c.MaxGroupBytes <= 0 {
		c.MaxGroupBytes = 256 << 20
	}
	if c.ClassTTL == nil {
		c.ClassTTL = DefaultClassTTL()
	}
	if c.PodsManifestTTL <= 0 {
		c.PodsManifestTTL = DefaultPodsManifestTTL()
	}
	return c
}

// NewJob builds a job over the store with the normalized config.
func NewJob(store ObjectStore, cfg Config) *Job {
	return &Job{store: store, cfg: cfg.Normalize()}
}

// Pass runs one maintenance round at the given wall-clock instant: per
// retention class, expire objects past their TTL (01 §6.4), then compact the
// settled bucket groups (01 §6.6); finally expire the pods/v1 manifests
// (01 §3.6). Failures on individual prefixes, groups, or objects are logged
// and counted in Stats.Errors so one bad object cannot stall the rest; the
// returned error is non-nil only when the context ends.
func (j *Job) Pass(ctx context.Context, now time.Time) (Stats, error) {
	var stats Stats
	for _, class := range model.RetentionClasses {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		objects, err := j.store.List(ctx, parquetPrefix+"/"+class+"/")
		if err != nil {
			stats.Errors++
			log.Error(ctx, err, "maintain: cannot list class %s", class)
			continue
		}
		files := make([]parquetObject, 0, len(objects))
		for _, obj := range objects {
			// The prefix may hold foreign objects; only 01 §7 keys take part.
			if po, ok := parseParquetKey(obj); ok {
				files = append(files, po)
			}
		}
		files = j.expireParquet(ctx, class, files, now, &stats)
		for _, group := range groupByBucket(files) {
			if err := ctx.Err(); err != nil {
				return stats, err
			}
			j.compactGroup(ctx, class, group, now, &stats)
		}
	}
	j.expirePodsManifests(ctx, now, &stats)
	return stats, ctx.Err()
}

// RunLoop runs Pass immediately and then on every interval tick until the
// context ends. The immediate first pass matters for a singleton with a long
// interval: a restarted maintainer must not sit idle for a full tick before
// resuming a half-finished compaction round.
func (j *Job) RunLoop(ctx context.Context, interval time.Duration) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		stats, err := j.Pass(ctx, time.Now())
		if err != nil && ctx.Err() == nil {
			log.Error(ctx, err, "maintain pass failed")
		}
		if j.OnPass != nil {
			j.OnPass(stats)
		}
		if stats != (Stats{}) {
			if stats.isNoteworthy() {
				log.Info(ctx, "maintain pass: %+v", stats)
			} else {
				// Routine skip counters (nothing settled/ready yet) run every
				// tick and would otherwise bury real events in steady-state
				// INFO volume (PR 708 review #27).
				log.Debug(ctx, "maintain pass: %+v", stats)
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
