package main

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/s3"
)

// s3Object is the slice of an S3 listing the §8.5 checks need.
type s3Object struct {
	Key  string // path-prefix already stripped
	Size int64
}

// objectLister lists every parquet object under the bucket's parquet/v1/
// prefix. The production implementation is read-only by contract
// (doc/checker.md): the checker must never create buckets or write objects.
type objectLister interface {
	List(ctx context.Context) ([]s3Object, error)
}

// minioLister is the production lister, built on s3.NewReadOnlyClient: the
// same connection parameters the collector reads, without the MakeBucket
// side effect of s3.NewClient.
type minioLister struct {
	client *s3.MinioClient
	prefix s3.KeyPrefix
}

func newMinioLister(ctx context.Context, params s3.Params, pathPrefix string) (*minioLister, error) {
	client, err := s3.NewReadOnlyClient(ctx, params)
	if err != nil {
		return nil, err
	}
	return &minioLister{client: client, prefix: s3.NewKeyPrefix(pathPrefix)}, nil
}

func (l *minioLister) List(ctx context.Context) ([]s3Object, error) {
	objects, err := l.client.ListObjectsWithPrefix(ctx, l.prefix.Apply("parquet/v1/"))
	if err != nil {
		return nil, err
	}
	out := make([]s3Object, 0, len(objects))
	for _, obj := range objects {
		key, ok := l.prefix.Strip(obj.Key)
		if !ok {
			continue
		}
		out = append(out, s3Object{Key: key, Size: obj.Size})
	}
	return out, nil
}

// parquetObjectKey is the parsed form of one 01 §7 object key. The parse
// rule mirrors libs/maintain (parseParquetKey): <replica> may contain
// dashes, so the name parses from the right; keys that do not parse are
// skipped — the prefix may hold foreign objects.
type parquetObjectKey struct {
	class         string
	hour          string // yyyy/mm/dd/hh, the hour prefix
	bucketStartMs int64
	replica       string
	size          int64
}

const s3KeyStamp = "20060102T150405Z"

func parseParquetObjectKey(obj s3Object) (parquetObjectKey, bool) {
	segs := strings.Split(obj.Key, "/")
	if len(segs) != 8 || segs[0] != "parquet" || segs[1] != "v1" {
		return parquetObjectKey{}, false
	}
	name, ok := strings.CutSuffix(segs[7], ".parquet")
	if !ok {
		return parquetObjectKey{}, false
	}
	parts := strings.Split(name, "-")
	if len(parts) < 6 {
		return parquetObjectKey{}, false
	}
	if _, err := strconv.Atoi(parts[len(parts)-1]); err != nil {
		return parquetObjectKey{}, false
	}
	bucketStart, err := time.Parse(s3KeyStamp, parts[len(parts)-4])
	if err != nil {
		return parquetObjectKey{}, false
	}
	replica := strings.Join(parts[:len(parts)-5], "-")
	if replica == "" {
		return parquetObjectKey{}, false
	}
	return parquetObjectKey{
		class:         segs[2],
		hour:          strings.Join(segs[3:7], "/"),
		bucketStartMs: bucketStart.UnixMilli(),
		replica:       replica,
		size:          obj.Size,
	}, true
}

// maintainReplica marks compacted outputs; only seal-produced objects count
// against the compaction trigger (libs/maintain writes outputs under this
// reserved replica token).
const maintainReplica = "maintain"

// s3Timers derives the §8.5 judgement deadlines from the stand's lifecycle
// timers, so the checker never demands compaction before maintain could have
// run (doc/checker.md).
type s3Timers struct {
	timeBucket            time.Duration
	timeBucketGrace       time.Duration
	sealCheckInterval     time.Duration
	uploadCheckInterval   time.Duration
	maintainCheckInterval time.Duration
	compactionMinAge      time.Duration
	compactionDeleteGrace time.Duration
	settleSlack           time.Duration
	compactionMinFiles    int
	smallFileBytes        int64
}

func (t s3Timers) objectsVisibleAt(bucketEnd time.Time) time.Time {
	return bucketEnd.Add(t.timeBucketGrace + t.sealCheckInterval + t.uploadCheckInterval + t.settleSlack)
}

func (t s3Timers) compactionDueAt(bucketEnd time.Time) time.Time {
	due := t.objectsVisibleAt(bucketEnd)
	if byAge := bucketEnd.Add(t.compactionMinAge); byAge.After(due) {
		due = byAge
	}
	// Two maintain intervals, not one: a group settling right after a pass
	// starts waits for the next pass to compact it, and the write → grace →
	// delete protocol removes its inputs one pass later still. Back-to-back
	// passes also run longer than the interval; the slack absorbs that.
	return due.Add(2*t.maintainCheckInterval + t.compactionDeleteGrace + t.settleSlack)
}

// settleChain is the whole seal-to-maintain deadline as a duration — the
// post-TTL grace the §8.7 -expect-ttl-deletion check waits before demanding
// a 404.
func (t s3Timers) settleChain() time.Duration {
	var zero time.Time
	return t.compactionDueAt(zero).Sub(zero)
}

// groupKey identifies one compaction unit: (bucket, retention_class).
type groupKey struct {
	class         string
	bucketStartMs int64
}

// hourKey identifies one hour prefix of one class.
type hourKey struct {
	class string
	hour  string
}

type hourStat struct {
	objects int
	small   int
	bytes   int64 // summed object size under this hour prefix
}

func (s hourStat) smallShare() float64 {
	if s.objects == 0 {
		return 0
	}
	return float64(s.small) / float64(s.objects)
}

// s3Sample is one listing, aggregated to the §8.5 units.
type s3Sample struct {
	at time.Time
	// sealed counts seal-produced objects per compaction group; compacted
	// outputs (replica "maintain") are excluded from the trigger check.
	sealed map[groupKey]int
	hours  map[hourKey]hourStat
	// lastBucketEndMs tracks, per hour, the end of the newest bucket seen in
	// it — the small-file check waits for the whole hour to pass its
	// compaction deadline.
	lastBucketEndMs map[hourKey]int64
}

func newS3Sample(at time.Time, objects []s3Object, timers s3Timers) s3Sample {
	s := s3Sample{
		at:              at,
		sealed:          map[groupKey]int{},
		hours:           map[hourKey]hourStat{},
		lastBucketEndMs: map[hourKey]int64{},
	}
	bucketMs := timers.timeBucket.Milliseconds()
	for _, obj := range objects {
		key, ok := parseParquetObjectKey(obj)
		if !ok {
			continue
		}
		hk := hourKey{class: key.class, hour: key.hour}
		stat := s.hours[hk]
		stat.objects++
		stat.bytes += key.size
		if key.size < timers.smallFileBytes {
			stat.small++
		}
		s.hours[hk] = stat
		if end := key.bucketStartMs + bucketMs; end > s.lastBucketEndMs[hk] {
			s.lastBucketEndMs[hk] = end
		}
		if key.replica != maintainReplica {
			s.sealed[groupKey{class: key.class, bucketStartMs: key.bucketStartMs}]++
		}
	}
	return s
}

// s3state keeps the listing history for the sliding-window checks.
type s3state struct {
	timers  s3Timers
	window  time.Duration
	samples []s3Sample
}

func newS3State(timers s3Timers, window time.Duration) *s3state {
	return &s3state{timers: timers, window: window}
}

func (s *s3state) append(sm s3Sample) {
	s.samples = append(s.samples, sm)
	horizon := sm.at.Add(-s.window)
	for len(s.samples) > 1 && s.samples[0].at.Before(horizon) {
		s.samples = s.samples[1:]
	}
}

// checkCompaction is §8.5 sub-invariant 1: every (bucket, class) group past
// its compaction deadline must hold fewer seal-produced objects than the
// compaction trigger — residue below the trigger is legal by design. A
// compaction-lag allowance shifts the deadlines by the closed fault windows'
// lengths; while such a window is open, groups are not judged at all
// (doc/checker.md, "Expected failures").
func (s *s3state) checkCompaction(now time.Time, faults *faultState) []finding {
	if len(s.samples) == 0 {
		return nil
	}
	if faults != nil && faults.hasOpen("compaction-lag") {
		return nil
	}
	last := s.samples[len(s.samples)-1]
	var out []finding
	for gk, count := range last.sealed {
		bucketEnd := time.UnixMilli(gk.bucketStartMs).Add(s.timers.timeBucket)
		due := s.timers.compactionDueAt(bucketEnd)
		if faults != nil {
			due = due.Add(faults.deadlineShift("compaction-lag", bucketEnd))
		}
		if now.Before(due) {
			continue
		}
		// The evidence must postdate the deadline: judging at due+ε with a
		// listing from due−75s reads a group maintain compacted seconds ago
		// as a miss. Every recurring one-shot §8.5 latch of the phase-5
		// fault runs was this race — count 1, gone by the next listing.
		if last.at.Before(due) {
			continue
		}
		if count >= s.timers.compactionMinFiles {
			out = append(out, finding{
				subject:    fmt.Sprintf("%s@%s", gk.class, time.UnixMilli(gk.bucketStartMs).UTC().Format(time.RFC3339)),
				observedAt: last.at,
				msg: fmt.Sprintf("%d sealed objects in a bucket past its compaction deadline (trigger is %d, deadline %s, listed %s)",
					count, s.timers.compactionMinFiles,
					due.UTC().Format(time.RFC3339),
					last.at.UTC().Format(time.RFC3339)),
			})
		}
	}
	sortFindings(out)
	return out
}

// checkSmallFileShare is §8.5 sub-invariant 2: once every bucket of an hour
// prefix is past its compaction deadline, the small-file share of that hour
// must not grow monotonically across the sliding window. Listings observed
// inside a small-file-share allowance window are not judged.
func (s *s3state) checkSmallFileShare(now time.Time, faults *faultState) []finding {
	if len(s.samples) < 2 {
		return nil
	}
	last := s.samples[len(s.samples)-1]
	var out []finding
	for hk := range last.hours {
		lastEndMs, ok := last.lastBucketEndMs[hk]
		if !ok {
			continue
		}
		hourDue := s.timers.compactionDueAt(time.UnixMilli(lastEndMs))
		if now.Before(hourDue) {
			continue
		}
		// The judged series starts only after the hour's deadline: shares
		// sampled while compaction was still legal to be pending prove
		// nothing.
		var shares []float64
		for _, sm := range s.samples {
			if sm.at.Before(hourDue) {
				continue
			}
			if faults != nil && faults.expected("small-file-share", sm.at) {
				continue
			}
			if stat, ok := sm.hours[hk]; ok {
				shares = append(shares, stat.smallShare())
			}
		}
		if err := monotonicGrowth(shares); err != nil {
			out = append(out, finding{
				subject:    hk.class + "/" + hk.hour,
				observedAt: last.at,
				msg:        "small-file share " + err.Error(),
			})
		}
	}
	sortFindings(out)
	return out
}

// logLine renders one listing for the checker log, so a soak report can cite
// the object-count, cold-byte, and small-file series per hour prefix.
func (sm s3Sample) logLine() string {
	keys := make([]hourKey, 0, len(sm.hours))
	for hk := range sm.hours {
		keys = append(keys, hk)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].class != keys[j].class {
			return keys[i].class < keys[j].class
		}
		return keys[i].hour < keys[j].hour
	})
	parts := make([]string, 0, len(keys))
	var totalBytes int64
	for _, hk := range keys {
		stat := sm.hours[hk]
		totalBytes += stat.bytes
		parts = append(parts, fmt.Sprintf("%s/%s: %d objects, %d bytes, %.0f%% small",
			hk.class, hk.hour, stat.objects, stat.bytes, stat.smallShare()*100))
	}
	parts = append(parts, fmt.Sprintf("total: %d bytes", totalBytes))
	if len(parts) == 0 {
		return "s3: no parquet objects"
	}
	return "s3: " + strings.Join(parts, "; ")
}

func sortFindings(fs []finding) {
	sort.Slice(fs, func(i, j int) bool { return fs[i].subject < fs[j].subject })
}
