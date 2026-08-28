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

// compactionInputsKey is the key-value footer entry of a compacted object
// listing the exact input keys it merged, newline-joined. The delete step
// reads it back instead of guessing the input set from the group shape, so
// several outputs of one oversized group (№11) each delete exactly their own
// inputs.
const compactionInputsKey = "profiler.compaction_inputs"

// compactGroup advances one (bucket, retention_class) group by one step of
// the write → grace → delete protocol of 01 §6.6. Each pass takes at most
// one kind of step, so every intermediate state is one the read path
// tolerates:
//
//  1. If the group holds maintain objects whose recorded inputs (the
//     compactionInputsKey footer entry) are still present, those inputs are
//     deleted once each output has been visible for DeleteGrace, else
//     nothing changes. A maintain object whose recorded inputs are all gone
//     is a settled member like any other.
//  2. Otherwise, if the group is settled and big enough, it is split into
//     sub-budget subgroups (№11: an oversized group compacts piecewise, it
//     is no longer skipped forever) and every subgroup merges into a fresh
//     compacted object. The inputs are NOT deleted here — the next pass
//     reads each output's recorded inputs and runs step 1.
func (j *Job) compactGroup(ctx context.Context, class string, group []parquetObject, now time.Time, stats *Stats) {
	if len(group) < 2 {
		return // one object (typically an earlier round's output) is already compact
	}
	present := make(map[string]parquetObject, len(group))
	for _, f := range group {
		present[f.key] = f
	}

	// Read every output's recorded inputs BEFORE any delete: an output can
	// itself be another output's input (a second-level compaction), and a
	// delete racing the footer reads would misread real state as an error.
	type pendingOutput struct {
		out     parquetObject
		pending []parquetObject
	}
	var pendingOutputs []pendingOutput
	handled := false
	for _, out := range group {
		if out.replica != producerToken {
			continue
		}
		inputs, err := j.readCompactionInputs(ctx, out)
		if err != nil {
			stats.Errors++
			log.Error(ctx, err, "maintain: cannot read the input list of %s", out.key)
			handled = true // do not recompact a group whose delete state is unknown
			continue
		}
		var pending []parquetObject
		for _, key := range inputs {
			if in, ok := present[key]; ok && key != out.key {
				pending = append(pending, in)
			}
		}
		if len(pending) == 0 {
			continue // settled output of an earlier round: an ordinary member
		}
		handled = true
		pendingOutputs = append(pendingOutputs, pendingOutput{out: out, pending: pending})
	}
	deleted := map[string]bool{}
	for _, po := range pendingOutputs {
		if now.Sub(po.out.lastModified) < j.cfg.DeleteGrace {
			stats.PendingDeleteGroups++
			continue
		}
		n := 0
		for _, in := range po.pending {
			if deleted[in.key] {
				continue // another output of this pass already covered it
			}
			if err := j.store.Delete(ctx, in.key); err != nil {
				stats.Errors++
				log.Error(ctx, err, "maintain: cannot delete compacted input %s", in.key)
				continue
			}
			deleted[in.key] = true
			stats.DeletedInputFiles++
			n++
		}
		log.Info(ctx, "maintain: deleted %d inputs of %s after the grace", n, po.out.key)
	}
	if handled {
		return
	}

	// A maintain object here is settled residue: stragglers arrived after
	// its round, or a delete was cut short. Recompact residue below MinFiles
	// so the bucket still converges.
	hasResidue := false
	for _, f := range group {
		if f.replica == producerToken {
			hasResidue = true
		}
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
	for _, sub := range j.splitByBudget(ctx, class, group, stats) {
		if len(sub) < 2 {
			continue // a lone sub-budget object is already as compact as it gets
		}
		if err := ctx.Err(); err != nil {
			return
		}
		j.compactSubgroup(ctx, class, sub, stats)
	}
}

// splitByBudget partitions a settled group into subgroups whose input bytes
// fit MaxGroupBytes (№11): members are packed in (timeMin, key) order, so
// each output covers a contiguous time slice of the bucket. A single member
// larger than the whole budget can never compact — it is the one remaining
// SkippedOversized case, and a sustained non-zero rate on that counter is
// the raise-MaxGroupBytes alert.
func (j *Job) splitByBudget(ctx context.Context, class string, group []parquetObject, stats *Stats) [][]parquetObject {
	sorted := append([]parquetObject(nil), group...)
	sort.Slice(sorted, func(a, b int) bool {
		if sorted[a].timeMinMs != sorted[b].timeMinMs {
			return sorted[a].timeMinMs < sorted[b].timeMinMs
		}
		return sorted[a].key < sorted[b].key
	})
	var subgroups [][]parquetObject
	var current []parquetObject
	var currentBytes int64
	for _, f := range sorted {
		if f.size > j.cfg.MaxGroupBytes {
			stats.SkippedOversized++
			log.Warning(ctx, "maintain: %s object %s alone exceeds the %d-byte budget; it stays uncompacted",
				class, f.key, j.cfg.MaxGroupBytes)
			continue
		}
		if len(current) > 0 && currentBytes+f.size > j.cfg.MaxGroupBytes {
			subgroups = append(subgroups, current)
			current, currentBytes = nil, 0
		}
		current = append(current, f)
		currentBytes += f.size
	}
	if len(current) > 0 {
		subgroups = append(subgroups, current)
	}
	return subgroups
}

// compactSubgroup merges one sub-budget input set into a fresh compacted
// object whose footer records the exact input keys for the delete step.
func (j *Job) compactSubgroup(ctx context.Context, class string, sub []parquetObject, stats *Stats) {
	res, err := j.streamCompacted(ctx, sub)
	if err != nil {
		stats.Errors++
		log.Error(ctx, err, "maintain: cannot merge %s bucket %s",
			class, time.UnixMilli(sub[0].bucketStartMs).UTC().Format(keyStamp))
		return
	}
	if res.rows == 0 {
		return
	}
	key := compactedKey(class, sub[0].bucketStartMs, inputsHash(sub), res.timeMinMs, res.timeMaxMs)
	if err := j.store.Put(ctx, key, res.body); err != nil {
		stats.Errors++
		log.Error(ctx, err, "maintain: cannot put %s", key)
		return
	}
	stats.CompactedGroups++
	stats.CompactedInputFiles += len(sub)
	stats.CompactedRows += res.rows
	stats.DedupedRows += res.deduped
	log.Info(ctx, "maintain: compacted %d objects into %s (%d rows, %d bytes)",
		len(sub), key, res.rows, len(res.body))
}

// readCompactionInputs reads a maintain object's recorded input keys from
// its footer metadata — one ranged footer read, no row data. An object
// without the entry reports no inputs: nothing pends on it.
func (j *Job) readCompactionInputs(ctx context.Context, f parquetObject) (inputs []string, err error) {
	obj, err := j.store.Open(ctx, f.key)
	if errors.Is(err, ErrNotFound) {
		return nil, nil // deleted since the LIST (another maintainer's grace step)
	}
	if err != nil {
		return nil, errors.Wrapf(err, "open %s", f.key)
	}
	defer func() { _ = obj.Close() }()
	defer func() {
		if r := recover(); r != nil {
			inputs, err = nil, errors.Errorf("read footer of %s: %v", f.key, r)
		}
	}()
	pf, err := parquetgo.OpenFile(obj, obj.Size(),
		parquetgo.SkipPageIndex(true), parquetgo.SkipBloomFilters(true))
	if err != nil {
		return nil, errors.Wrapf(err, "read parquet footer of %s", f.key)
	}
	joined, ok := pf.Lookup(compactionInputsKey)
	if !ok || joined == "" {
		return nil, nil
	}
	return strings.Split(joined, "\n"), nil
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

// mergeBatchRows sizes the per-input read-ahead and the writer batches of
// the streaming merge: peak memory is O(inputs × mergeBatchRows) rows, not
// the whole group (№11).
const mergeBatchRows = 256

// compactResult is one streamed subgroup merge: the finished parquet body
// plus the stamps and counters the key and stats need.
type compactResult struct {
	body      []byte
	rows      int
	deduped   int
	timeMinMs int64
	timeMaxMs int64
}

// mergeCursor streams one input object batch by batch. The library reports
// an unconvertible file schema by panicking inside the reader; next recovers
// it into this object's error so a foreign or future-versioned file degrades
// to a skipped subgroup, not a crashed job.
type mergeCursor struct {
	key  string
	obj  Object
	r    *parquetgo.GenericReader[storageparquet.CallV2]
	buf  []storageparquet.CallV2
	pos  int
	n    int
	done bool
}

func (j *Job) openCursor(ctx context.Context, f parquetObject) (c *mergeCursor, err error) {
	obj, err := j.store.Open(ctx, f.key)
	if err != nil {
		return nil, errors.Wrapf(err, "open %s", f.key)
	}
	defer func() {
		if r := recover(); r != nil {
			_ = obj.Close()
			c, err = nil, errors.Errorf("read %s: %v", f.key, r)
		}
	}()
	pf, err := parquetgo.OpenFile(obj, obj.Size(),
		parquetgo.SkipPageIndex(true), parquetgo.SkipBloomFilters(true))
	if err != nil {
		_ = obj.Close()
		return nil, errors.Wrapf(err, "read parquet footer of %s", f.key)
	}
	return &mergeCursor{
		key: f.key, obj: obj,
		r:   parquetgo.NewGenericReader[storageparquet.CallV2](pf),
		buf: make([]storageparquet.CallV2, mergeBatchRows),
	}, nil
}

// head returns the cursor's current row; ok is false once the input is
// drained.
func (c *mergeCursor) head() (row *storageparquet.CallV2, ok bool, err error) {
	if c.pos < c.n {
		return &c.buf[c.pos], true, nil
	}
	if c.done {
		return nil, false, nil
	}
	defer func() {
		if r := recover(); r != nil {
			row, ok, err = nil, false, errors.Errorf("read %s: %v", c.key, r)
		}
	}()
	n, err := c.r.Read(c.buf)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, false, errors.Wrapf(err, "read %s", c.key)
	}
	c.pos, c.n = 0, n
	if n == 0 {
		c.done = true
		return nil, false, nil
	}
	return &c.buf[0], true, nil
}

func (c *mergeCursor) advance() { c.pos++ }

func (c *mergeCursor) close() {
	if c.r != nil {
		_ = c.r.Close()
	}
	_ = c.obj.Close()
}

// streamCompacted k-way merges the subgroup's already-sorted inputs into one
// parquet body in the global (ts_ms DESC, pk ASC) order of 01 §5.2, with
// PK-dedup: idempotent overlaps — an earlier round's output next to its
// still-undeleted inputs — duplicate whole rows, and 01 §6.2 makes the
// copies identical, so keeping the first is safe. Rows stream from the
// inputs to the writer batch by batch (№11): memory holds one read-ahead
// batch per input plus the writer state, never the whole group. The output
// footer records the input keys for the delete step.
func (j *Job) streamCompacted(ctx context.Context, sub []parquetObject) (res compactResult, err error) {
	cursors := make([]*mergeCursor, 0, len(sub))
	defer func() {
		for _, c := range cursors {
			c.close()
		}
	}()
	keys := make([]string, 0, len(sub))
	for _, f := range sub {
		c, err := j.openCursor(ctx, f)
		if err != nil {
			return compactResult{}, err
		}
		cursors = append(cursors, c)
		keys = append(keys, f.key)
	}
	sort.Strings(keys)

	var buf bytes.Buffer
	w := parquetgo.NewGenericWriter[storageparquet.CallV2](&buf,
		append(storageparquet.CallV2WriterOptions(),
			parquetgo.KeyValueMetadata(compactionInputsKey, strings.Join(keys, "\n")))...)
	defer func() {
		if r := recover(); r != nil {
			res, err = compactResult{}, errors.Errorf("write compacted rows: %v", r)
		}
	}()

	var last storageparquet.CallV2
	batch := make([]storageparquet.CallV2, 0, mergeBatchRows)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if _, err := w.Write(batch); err != nil {
			return errors.Wrap(err, "write compacted rows")
		}
		batch = batch[:0]
		return nil
	}
	for {
		if err := ctx.Err(); err != nil {
			return compactResult{}, err
		}
		// A linear min-scan over the cursor heads: subgroup sizes are tens of
		// objects at most, so the scan is cheaper than heap bookkeeping.
		var best *mergeCursor
		var bestRow *storageparquet.CallV2
		for _, c := range cursors {
			row, ok, err := c.head()
			if err != nil {
				return compactResult{}, err
			}
			if !ok {
				continue
			}
			if bestRow == nil || rowCompare(row, bestRow) < 0 {
				best, bestRow = c, row
			}
		}
		if bestRow == nil {
			break
		}
		if res.rows > 0 && rowCompare(&last, bestRow) == 0 {
			res.deduped++
			best.advance()
			continue
		}
		last = *bestRow
		batch = append(batch, last)
		if res.rows == 0 || last.TsMs < res.timeMinMs {
			res.timeMinMs = last.TsMs
		}
		if res.rows == 0 || last.TsMs > res.timeMaxMs {
			res.timeMaxMs = last.TsMs
		}
		res.rows++
		best.advance()
		if len(batch) == mergeBatchRows {
			if err := flush(); err != nil {
				return compactResult{}, err
			}
		}
	}
	if res.rows == 0 {
		return res, nil
	}
	if err := flush(); err != nil {
		return compactResult{}, err
	}
	if err := w.Close(); err != nil {
		return compactResult{}, errors.Wrap(err, "finish compacted parquet")
	}
	res.body = buf.Bytes()
	return res, nil
}
