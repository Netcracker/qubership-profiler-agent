package cold

import (
	"context"
	"fmt"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
)

// keyStamp is the second-precision UTC stamp of the sealed-file name
// (01-write-contract.md §7); it must match hotstore's sealedNameStamp.
const keyStamp = "20060102T150405Z"

// MaintainReplica is the reserved <replica> token of a compacted object key
// (01-write-contract.md §7). A cross-pod-restart compaction keys its output
// by the hash of its inputs, not by any one pod-restart, so the point-fetch
// path (scan.go) cannot prune such a file by hash and must read it whole for
// every PK.
const MaintainReplica = "maintain"

// cleanClassUpperMs derives the §5.5 pruning bounds from the SAME tier table
// (and the same threshold override) the write side classifies with (№10) —
// never from a second hardcode: a read-side copy that drifted from the seal
// classification silently dropped whole classes from cold results. The error
// classes carry calls of any duration and are never pruned by a duration
// filter (02 §2.3.2).
func (s *Source) cleanClassUpperMs() map[string]int64 {
	return model.CleanClassUpperMs(s.DurationThresholds)
}

type (
	// FileRef is one discovered parquet candidate: everything the read plan
	// needs, obtained from the LIST alone (02 §5.1). Hash is the key's
	// pod-restart hash — the point-fetch path uses it to skip other
	// pod-restarts' files without opening them.
	FileRef struct {
		Key       string
		Size      int64
		Class     string
		Replica   string
		Hash      string
		TimeMinMs int64
		TimeMaxMs int64
	}

	// Discovery is one query's cold read plan plus the §2.3.2 cost estimate
	// and the §7.4 partial markers.
	Discovery struct {
		Files          []FileRef
		PartialReasons []string // failed LIST prefixes
		Prefixes       int      // prefixes attempted
		FailedPrefixes int
	}
)

// ClassesFor prunes the retention classes a query has to list (02 §2.3.2,
// §5.5): an explicit retention_class filter selects key prefixes verbatim,
// error_only keeps only the error classes, and duration_min_ms drops every
// clean class whose duration range sits entirely below the threshold.
func (s *Source) ClassesFor(q model.CallsQuery) []string {
	cleanUpperMs := s.cleanClassUpperMs()
	classes := q.RetentionClasses
	if len(classes) == 0 {
		classes = model.RetentionClasses
	}
	var out []string
	for _, c := range classes {
		if q.ErrorOnly && c != model.RetentionAnyError && c != model.RetentionCorrupted {
			continue
		}
		if upper, clean := cleanUpperMs[c]; clean && int64(q.DurationMinMs) >= upper {
			continue
		}
		out = append(out, c)
	}
	return out
}

// Discover walks the hour prefixes covering [q.FromMs, q.ToMs) for every
// non-pruned class, LISTs them in parallel, and keeps the files whose
// key-encoded [timeMin, timeMax] overlaps the window (02 §5.1: timeMin < to
// AND timeMax >= from). No footer is read and no HEAD is issued. A failed
// LIST becomes a partial reason instead of failing the query (§7.4).
func (s *Source) Discover(ctx context.Context, q model.CallsQuery) (Discovery, error) {
	var prefixes []string
	for _, class := range s.ClassesFor(q) {
		for _, hour := range hourWalk(q.FromMs, q.ToMs) {
			prefixes = append(prefixes,
				path.Join("parquet/v1", class, hour.Format("2006/01/02/15"))+"/")
		}
	}

	d := Discovery{Prefixes: len(prefixes)}
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, s.listConcurrency())
	for _, prefix := range prefixes {
		wg.Add(1)
		go func(prefix string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			objects, err := s.Store.List(ctx, prefix)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				d.FailedPrefixes++
				d.PartialReasons = append(d.PartialReasons,
					fmt.Sprintf("s3 list %s: %v", prefix, err))
				return
			}
			for _, obj := range objects {
				ref, ok := ParseKey(obj.Key, obj.Size)
				if !ok {
					continue
				}
				if ref.TimeMinMs < q.ToMs && ref.TimeMaxMs >= q.FromMs {
					d.Files = append(d.Files, ref)
				}
			}
		}(prefix)
	}
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return d, err
	}
	return d, nil
}

// hourWalk lists the UTC hours between floor(from, 1h) and ceil(to, 1h)
// (02 §5.1 steps 2-3). Rows are filed by the bucket of their start and a
// bucket never spans an hour boundary, so earlier hour prefixes cannot hold
// an overlapping file.
func hourWalk(fromMs, toMs int64) []time.Time {
	if toMs <= fromMs {
		return nil
	}
	first := time.UnixMilli(fromMs).UTC().Truncate(time.Hour)
	last := time.UnixMilli(toMs - 1).UTC().Truncate(time.Hour)
	var hours []time.Time
	for h := first; !h.After(last); h = h.Add(time.Hour) {
		hours = append(hours, h)
	}
	return hours
}

// ParseKey decodes one 01 §7 object key:
//
//	parquet/v1/<class>/<yyyy>/<mm>/<dd>/<hh>/<replica>-<hash>-<bucketStart>-<timeMin>-<timeMax>-<seq>.parquet
//
// <replica> may itself contain dashes (collector-0), so the name is parsed
// from the right. A key that does not parse is skipped by discovery: the
// prefix may legitimately hold foreign objects.
func ParseKey(key string, size int64) (FileRef, bool) {
	segs := strings.Split(key, "/")
	if len(segs) != 8 || segs[0] != "parquet" || segs[1] != "v1" {
		return FileRef{}, false
	}
	class := segs[2]
	if !model.IsRetentionClass(class) {
		return FileRef{}, false
	}
	name, ok := strings.CutSuffix(segs[7], ".parquet")
	if !ok {
		return FileRef{}, false
	}
	parts := strings.Split(name, "-")
	if len(parts) < 6 {
		return FileRef{}, false
	}
	if _, err := strconv.Atoi(parts[len(parts)-1]); err != nil {
		return FileRef{}, false
	}
	timeMin, err := time.Parse(keyStamp, parts[len(parts)-3])
	if err != nil {
		return FileRef{}, false
	}
	timeMax, err := time.Parse(keyStamp, parts[len(parts)-2])
	if err != nil {
		return FileRef{}, false
	}
	if _, err := time.Parse(keyStamp, parts[len(parts)-4]); err != nil {
		return FileRef{}, false // the bucket-start stamp
	}
	return FileRef{
		Key:   key,
		Size:  size,
		Class: class,
		// <replica> may itself contain dashes (collector-0), so it is
		// everything left of the hash. A compacted object carries the reserved
		// MaintainReplica token here (01 §7); the point-fetch path keys off it.
		Replica: strings.Join(parts[:len(parts)-5], "-"),
		Hash:    parts[len(parts)-5],
		// The key stamps are second-precision (01 §7) while row ts_ms is
		// milliseconds: both bounds are truncated downward in the key. Widen
		// timeMax to the end of its second so a row in the truncated tail
		// cannot slip past the overlap test; the floor of timeMin errs on the
		// safe (inclusive) side already.
		TimeMinMs: timeMin.UnixMilli(),
		TimeMaxMs: timeMax.UnixMilli() + 999,
	}, true
}
