package maintain

import (
	"context"
	"strings"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
)

// podsManifestRoot is the pods/v1 identity-manifest family next to the
// parquet data (01-write-contract.md §3.6) — the only snapshot family left:
// the dictionary and suspend snapshots are gone since the parquet rows
// became self-contained (№3, №23).
const podsManifestRoot = "pods/v1"

// expireParquet deletes the class's objects whose newest possible row is
// older than the class TTL, judged by the key's timeMax stamp alone — no
// footer is read (01 §6.4). timeMaxMs is already widened to the end of its
// second, so the comparison errs on the keep side: an object is deleted only
// when every row it can hold has expired, never inside its TTL. Survivors
// are returned for the compaction planner.
func (j *Job) expireParquet(ctx context.Context, class string, files []parquetObject, now time.Time, stats *Stats) []parquetObject {
	ttl, ok := j.cfg.ClassTTL[class]
	if !ok {
		return files
	}
	cutoffMs := now.Add(-ttl).UnixMilli()
	kept := files[:0]
	for _, f := range files {
		if f.timeMaxMs >= cutoffMs {
			kept = append(kept, f)
			continue
		}
		if err := j.store.Delete(ctx, f.key); err != nil {
			stats.Errors++
			log.Error(ctx, err, "maintain: cannot delete expired %s", f.key)
			kept = append(kept, f)
			continue
		}
		stats.TTLParquetDeleted++
		log.Info(ctx, "maintain: deleted expired %s (ttl %s)", f.key, ttl)
	}
	return kept
}

// expirePodsManifests deletes pods/v1 manifests whose UTC day — the only
// time the key carries (01 §3.6) — ended more than PodsManifestTTL ago.
// Aging from the day's end keeps every object through its full TTL
// regardless of when within the day it was written.
func (j *Job) expirePodsManifests(ctx context.Context, now time.Time, stats *Stats) {
	objects, err := j.store.List(ctx, podsManifestRoot+"/")
	if err != nil {
		stats.Errors++
		log.Error(ctx, err, "maintain: cannot list %s", podsManifestRoot)
		return
	}
	for _, obj := range objects {
		if err := ctx.Err(); err != nil {
			return
		}
		dayEnd, ok := manifestDayEnd(podsManifestRoot, obj.Key)
		if !ok {
			continue // foreign object under the prefix
		}
		if now.Sub(dayEnd) <= j.cfg.PodsManifestTTL {
			continue
		}
		if err := j.store.Delete(ctx, obj.Key); err != nil {
			stats.Errors++
			log.Error(ctx, err, "maintain: cannot delete expired manifest %s", obj.Key)
			continue
		}
		stats.TTLManifestsDeleted++
		log.Info(ctx, "maintain: deleted expired manifest %s", obj.Key)
	}
}

// manifestDayEnd parses `<root>/<yyyy>/<mm>/<dd>/<name>.json` and returns the
// end of the key's UTC day.
func manifestDayEnd(root, key string) (time.Time, bool) {
	rest, ok := strings.CutPrefix(key, root+"/")
	if !ok {
		return time.Time{}, false
	}
	segs := strings.Split(rest, "/")
	if len(segs) != 4 || !strings.HasSuffix(segs[3], ".json") {
		return time.Time{}, false
	}
	day, err := time.Parse("2006/01/02", strings.Join(segs[:3], "/"))
	if err != nil {
		return time.Time{}, false
	}
	return day.Add(24 * time.Hour), true
}
