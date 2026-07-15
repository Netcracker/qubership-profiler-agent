package maintain

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	storageparquet "github.com/Netcracker/qubership-profiler-backend/libs/storage/parquet"
	parquetgo "github.com/parquet-go/parquet-go"
	"github.com/pkg/errors"
)

// keyStamp is the second-precision UTC stamp of the sealed-file name
// (01-write-contract.md §7); it matches hotstore's sealedNameStamp and
// cold's keyStamp.
const keyStamp = "20060102T150405Z"

// parquetObject is one parsed 01 §7 key plus its LIST metadata.
type parquetObject struct {
	key           string
	size          int64
	lastModified  time.Time
	replica       string
	hash          string
	bucketStartMs int64
	timeMinMs     int64
	// timeMaxMs is widened to the end of its second, mirroring the cold
	// reader's ParseKey: both key stamps truncate milliseconds downward, so
	// the raw stamp understates the newest row by up to 999 ms.
	timeMaxMs int64
	seq       int
}

// parseParquetKey decodes one 01 §7 object key:
//
//	parquet/v1/<class>/<yyyy>/<mm>/<dd>/<hh>/<replica>-<hash>-<bucketStart>-<timeMin>-<timeMax>-<seq>.parquet
//
// <replica> may itself contain dashes (collector-0), so the name parses from
// the right. A key that does not parse is skipped: the prefix may hold
// foreign objects, and deleting or compacting what we cannot read is worse
// than leaving it alone.
func parseParquetKey(obj ObjectInfo) (parquetObject, bool) {
	segs := strings.Split(obj.Key, "/")
	if len(segs) != 8 || segs[0] != "parquet" || segs[1] != "v1" {
		return parquetObject{}, false
	}
	if !model.IsRetentionClass(segs[2]) {
		return parquetObject{}, false
	}
	name, ok := strings.CutSuffix(segs[7], ".parquet")
	if !ok {
		return parquetObject{}, false
	}
	parts := strings.Split(name, "-")
	if len(parts) < 6 {
		return parquetObject{}, false
	}
	seq, err := strconv.Atoi(parts[len(parts)-1])
	if err != nil {
		return parquetObject{}, false
	}
	timeMax, err := time.Parse(keyStamp, parts[len(parts)-2])
	if err != nil {
		return parquetObject{}, false
	}
	timeMin, err := time.Parse(keyStamp, parts[len(parts)-3])
	if err != nil {
		return parquetObject{}, false
	}
	bucketStart, err := time.Parse(keyStamp, parts[len(parts)-4])
	if err != nil {
		return parquetObject{}, false
	}
	replica := strings.Join(parts[:len(parts)-5], "-")
	if replica == "" {
		return parquetObject{}, false
	}
	return parquetObject{
		key:           obj.Key,
		size:          obj.Size,
		lastModified:  obj.LastModified,
		replica:       replica,
		hash:          parts[len(parts)-5],
		bucketStartMs: bucketStart.UnixMilli(),
		timeMinMs:     timeMin.UnixMilli(),
		timeMaxMs:     timeMax.UnixMilli() + 999,
		seq:           seq,
	}, true
}

// groupByBucket splits one class's files into per-bucket groups — the
// compaction unit of 01 §6.6 is (bucket, retention_class). Groups and their
// members come back in a deterministic order so two maintainers walk the
// same plan.
func groupByBucket(files []parquetObject) [][]parquetObject {
	byBucket := map[int64][]parquetObject{}
	for _, f := range files {
		byBucket[f.bucketStartMs] = append(byBucket[f.bucketStartMs], f)
	}
	starts := make([]int64, 0, len(byBucket))
	for start := range byBucket {
		starts = append(starts, start)
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i] < starts[j] })
	groups := make([][]parquetObject, 0, len(starts))
	for _, start := range starts {
		group := byBucket[start]
		sort.Slice(group, func(i, j int) bool { return group[i].key < group[j].key })
		groups = append(groups, group)
	}
	return groups
}

// inputsHash renders the short hash that substitutes for the pod-restart
// hash in a compacted object's key (01 §7). It hashes the sorted input keys,
// so any maintainer compacting the same input set produces the same output
// key — the PUT stays idempotent even across a singleton violation — and a
// later pass recognises its own output by recomputing the hash over the
// remaining group members.
func inputsHash(files []parquetObject) string {
	keys := make([]string, len(files))
	for i, f := range files {
		keys[i] = f.key
	}
	sort.Strings(keys)
	sum := sha256.Sum256([]byte(strings.Join(keys, "\n")))
	return hex.EncodeToString(sum[:4])
}

// compactedKey renders the 01 §7 key of a compacted object: the reserved
// producer token, the hash of the inputs in the pod-restart slot, and the
// same truncate-to-second stamps the seal pass writes — discovery widens
// timeMax back to the end of its second at parse (02 §5.1).
func compactedKey(class string, bucketStartMs int64, hash string, timeMinMs, timeMaxMs int64) string {
	bucketStart := time.UnixMilli(bucketStartMs).UTC()
	name := fmt.Sprintf("%s-%s-%s-%s-%s-0.parquet",
		producerToken, hash,
		bucketStart.Format(keyStamp),
		time.UnixMilli(timeMinMs).UTC().Format(keyStamp),
		time.UnixMilli(timeMaxMs).UTC().Format(keyStamp))
	return path.Join(parquetPrefix, class, bucketStart.Format("2006/01/02/15"), name)
}

// compactGroup advances one (bucket, retention_class) group by one step of
// the write → grace → delete protocol of 01 §6.6. Each pass takes at most
// one step, so every intermediate state is one the read path tolerates:
//
//  1. If the group holds a maintain object whose hash matches the remaining
//     members, that round's output is written; delete the inputs once the
//     output has been visible for DeleteGrace, else leave everything alone.
//  2. Otherwise, if the group is settled and big enough, merge every member
//     into a fresh compacted object. The inputs are NOT deleted here — the
//     next pass finds the output by its hash and runs step 1.
func (j *Job) compactGroup(ctx context.Context, class string, group []parquetObject, now time.Time, stats *Stats) {
	if len(group) < 2 {
		return // one object (typically an earlier round's output) is already compact
	}

	for i := range group {
		out := group[i]
		if out.replica != producerToken {
			continue
		}
		rest := make([]parquetObject, 0, len(group)-1)
		rest = append(append(rest, group[:i]...), group[i+1:]...)
		if out.hash != inputsHash(rest) {
			continue
		}
		if now.Sub(out.lastModified) < j.cfg.DeleteGrace {
			stats.PendingDeleteGroups++
			return
		}
		for _, in := range rest {
			if err := j.store.Delete(ctx, in.key); err != nil {
				stats.Errors++
				log.Error(ctx, err, "maintain: cannot delete compacted input %s", in.key)
				continue
			}
			stats.DeletedInputFiles++
		}
		log.Info(ctx, "maintain: deleted %d inputs of %s after the grace", len(rest), out.key)
		return
	}

	// A maintain object that did not match above is residue: stragglers
	// arrived after its round, or a delete was cut short. Recompact residue
	// below MinFiles so the bucket still converges to one object.
	hasResidue := false
	var totalBytes int64
	for _, f := range group {
		if f.replica == producerToken {
			hasResidue = true
		}
		totalBytes += f.size
	}
	if len(group) < j.cfg.MinFiles && !hasResidue {
		stats.SkippedSmallGroups++
		return
	}
	bucketEnd := time.UnixMilli(group[0].bucketStartMs).Add(j.cfg.TimeBucket)
	if now.Before(bucketEnd.Add(j.cfg.MinAge)) {
		stats.SkippedUnsettled++
		return
	}
	if totalBytes > j.cfg.MaxGroupBytes {
		stats.SkippedOversized++
		log.Warning(ctx, "maintain: skipping %s bucket %s: %d input bytes exceed the %d budget",
			class, time.UnixMilli(group[0].bucketStartMs).UTC().Format(keyStamp), totalBytes, j.cfg.MaxGroupBytes)
		return
	}

	rows, deduped, err := j.readMerged(ctx, group)
	if err != nil {
		stats.Errors++
		log.Error(ctx, err, "maintain: cannot merge %s bucket %s",
			class, time.UnixMilli(group[0].bucketStartMs).UTC().Format(keyStamp))
		return
	}
	if len(rows) == 0 {
		return
	}
	body, timeMinMs, timeMaxMs, err := writeCompacted(rows)
	if err != nil {
		stats.Errors++
		log.Error(ctx, err, "maintain: cannot write the compacted object for %s bucket %s",
			class, time.UnixMilli(group[0].bucketStartMs).UTC().Format(keyStamp))
		return
	}
	key := compactedKey(class, group[0].bucketStartMs, inputsHash(group), timeMinMs, timeMaxMs)
	if err := j.store.Put(ctx, key, body); err != nil {
		stats.Errors++
		log.Error(ctx, err, "maintain: cannot put %s", key)
		return
	}
	stats.CompactedGroups++
	stats.CompactedInputFiles += len(group)
	stats.CompactedRows += len(rows)
	stats.DedupedRows += deduped
	log.Info(ctx, "maintain: compacted %d objects into %s (%d rows, %d bytes)",
		len(group), key, len(rows), len(body))
}

// readMerged materializes every group member and restores the global
// (ts_ms DESC, pk ASC) order of 01 §5.2 with PK-dedup: idempotent overlaps —
// an earlier round's output next to its still-undeleted inputs — duplicate
// whole rows, and 01 §6.2 makes the copies identical, so keeping the first
// is safe.
func (j *Job) readMerged(ctx context.Context, group []parquetObject) ([]storageparquet.CallV2, int, error) {
	var all []storageparquet.CallV2
	for _, f := range group {
		rows, err := j.readRows(ctx, f)
		if err != nil {
			return nil, 0, err
		}
		all = append(all, rows...)
	}
	sort.SliceStable(all, func(a, b int) bool { return rowCompare(&all[a], &all[b]) < 0 })
	deduped := 0
	kept := all[:0]
	for i := range all {
		if len(kept) > 0 && rowCompare(&kept[len(kept)-1], &all[i]) == 0 {
			deduped++
			continue
		}
		kept = append(kept, all[i])
	}
	return kept, deduped, nil
}

// rowCompare orders rows by (ts_ms DESC, pk ASC) — the 01 §5.2 file order —
// through the shared component-wise PK collation (02 §2.3.1). Zero means the
// same call: the PK is unique per call and ts_ms is a function of it.
func rowCompare(a, b *storageparquet.CallV2) int {
	if a.TsMs != b.TsMs {
		if a.TsMs > b.TsMs {
			return -1
		}
		return 1
	}
	return rowPK(a).Compare(rowPK(b))
}

func rowPK(r *storageparquet.CallV2) model.PK {
	return model.PK{
		PodNamespace:   r.Namespace,
		PodService:     r.ServiceName,
		PodName:        r.PodName,
		RestartTimeMs:  r.RestartTimeMs,
		TraceFileIndex: r.TraceFileIndex,
		BufferOffset:   r.BufferOffset,
		RecordIndex:    r.RecordIndex,
	}
}

// readRows materializes one object through the full CallV2 schema, so every
// column — trace_blob and big_params_json included — survives the rewrite.
// The library reports an unconvertible file schema by panicking inside the
// reader; recover it into this object's error so a foreign or
// future-versioned file degrades to a skipped group, not a crashed job.
func (j *Job) readRows(ctx context.Context, f parquetObject) (rows []storageparquet.CallV2, err error) {
	obj, err := j.store.Open(ctx, f.key)
	if err != nil {
		return nil, errors.Wrapf(err, "open %s", f.key)
	}
	defer func() { _ = obj.Close() }()
	defer func() {
		if r := recover(); r != nil {
			rows, err = nil, errors.Errorf("read %s: %v", f.key, r)
		}
	}()

	pf, err := parquetgo.OpenFile(obj, obj.Size(),
		parquetgo.SkipPageIndex(true), parquetgo.SkipBloomFilters(true))
	if err != nil {
		return nil, errors.Wrapf(err, "read parquet footer of %s", f.key)
	}
	r := parquetgo.NewGenericReader[storageparquet.CallV2](pf)
	defer func() { _ = r.Close() }()

	rows = make([]storageparquet.CallV2, r.NumRows())
	n, err := r.Read(rows)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, errors.Wrapf(err, "read %s", f.key)
	}
	if n != len(rows) {
		return nil, errors.Errorf("read %s: footer promises %d rows, read %d", f.key, len(rows), n)
	}
	return rows, nil
}

// writeCompacted renders the merged rows into one parquet body with the
// shared CallV2 writer invariants (ZSTD, schema-version stamp, no page
// bounds on the blob-sized columns) and reports the true min/max ts_ms for
// the key stamps.
func writeCompacted(rows []storageparquet.CallV2) (body []byte, timeMinMs, timeMaxMs int64, err error) {
	timeMinMs, timeMaxMs = rows[0].TsMs, rows[0].TsMs
	for i := range rows {
		if rows[i].TsMs < timeMinMs {
			timeMinMs = rows[i].TsMs
		}
		if rows[i].TsMs > timeMaxMs {
			timeMaxMs = rows[i].TsMs
		}
	}
	var buf bytes.Buffer
	w := parquetgo.NewGenericWriter[storageparquet.CallV2](&buf, storageparquet.CallV2WriterOptions()...)
	if _, err := w.Write(rows); err != nil {
		return nil, 0, 0, errors.Wrap(err, "write compacted rows")
	}
	if err := w.Close(); err != nil {
		return nil, 0, 0, errors.Wrap(err, "finish compacted parquet")
	}
	return buf.Bytes(), timeMinMs, timeMaxMs, nil
}
