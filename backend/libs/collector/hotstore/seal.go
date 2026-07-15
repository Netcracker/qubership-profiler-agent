package hotstore

import (
	"compress/gzip"
	"context"
	"encoding/binary"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/Netcracker/qubership-profiler-backend/libs/parser/pipe"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	storageparquet "github.com/Netcracker/qubership-profiler-backend/libs/storage/parquet"
	"github.com/parquet-go/parquet-go"
	"github.com/pkg/errors"
)

// Truncation reasons for trace_blob = NULL (01-write-contract.md §5.2).
// mem_pressure is reserved for the memory-budget janitor (§4.6); the seal pass
// itself never emits it in this slice (TODO seam: budgets task).
const (
	TruncDictMiss    = "dict_miss"
	TruncDiskBudget  = "disk_budget"
	TruncIdleTimeout = "idle_timeout"
	TruncMemPressure = "mem_pressure"
)

// sealedNameStamp renders the second-precision UTC stamps of the sealed file
// name (01-write-contract.md §7).
const sealedNameStamp = "20060102T150405Z"

type (
	// SealedFile describes one parquet file a seal pass produced.
	SealedFile struct {
		Path           string // local sealed path, mirrors the S3 key under DataDir
		S3Key          string // deterministic S3 object key (01 §7); the Uploader PUTs it verbatim
		RetentionClass string
		Seq            int
		Rows           int
		TimeMinMs      int64
		TimeMaxMs      int64
	}

	// SealResult reports what one Seal call materialized.
	SealResult struct {
		Files     []SealedFile
		Rows      int
		Truncated map[string]int // truncated_reason → row count
	}

	// SealCounters are process-lifetime seal metrics; they back the
	// profiler_seal_* Prometheus series (apps/profiler-backend/pkg/metrics).
	SealCounters struct {
		Rows      int64
		Files     int64
		Truncated map[string]int64
		// LostBigValues counts big-parameter values a seal could not resolve
		// because their value segment was evicted or torn (№7); each loss also
		// truncates its row with disk_budget, so it is never silent.
		LostBigValues int64
		// WalBytesRead counts the calls.wal bytes seal passes fetched; the №9
		// ReaderAt path reads only the referenced records, so this stays around
		// the total WAL size, not passes × WAL size.
		WalBytesRead int64
	}

	// sealRow carries one call through the pass: the index row, the full WAL
	// record, the blob assembly outcome, the big-parameter references the
	// blob carries (resolved into big_params_json before the row is written,
	// because the value segments never reach S3), and the dictionary ids the
	// blob's events reference (resolved into dict_words_json, so the sealed
	// row is self-contained — №3, №23).
	sealRow struct {
		idx       CallIndexRow
		wal       CallWalRecord
		blob      *blobBuffer
		truncated string
		srcSegs   map[segKey]struct{}
		bigRefs   []ValueRef
		bigValues map[string]string
		dictIds   map[int]struct{}
		// blobTimeMinMs/blobTimeMaxMs span the call's own events on the trace
		// timer axis (§4.2) — the axis the tree renders node windows on. The
		// suspend_json filter uses this span, NOT [ts_ms, ts_ms+duration]:
		// ts_ms comes from the calls stream's independent epoch, and a small
		// divergence between the two clocks must not drop a boundary pause.
		blobTimeMinMs int64
		blobTimeMaxMs int64
		hasBlobTime   bool
	}

	// segKey addresses one hot-store segment within a pod-restart.
	segKey struct {
		stream string
		seq    int
	}

	// assembly is the per-call state while its chunks stream past the segment
	// cursor (01-write-contract.md §6.5).
	assembly struct {
		row      *sealRow
		threadId uint64
		started  bool
		depth    int
		done     bool
	}
)

// PodRestartHash renders the short pod-restart hash used in sealed-file names
// and S3 keys (01-write-contract.md §7). The hash itself lives in the shared
// model package so the cold read path resolves the same keys.
func PodRestartHash(key PodRestartKey) string {
	return model.PodRestartHash(key.Tuple())
}

// SealCountersSnapshot returns a copy of the process-lifetime seal metrics.
func (s *Store) SealCountersSnapshot() SealCounters {
	s.sealMu.Lock()
	defer s.sealMu.Unlock()
	out := SealCounters{
		Rows:          s.sealCounters.Rows,
		Files:         s.sealCounters.Files,
		Truncated:     make(map[string]int64, len(s.sealCounters.Truncated)),
		LostBigValues: s.sealCounters.LostBigValues,
		WalBytesRead:  s.sealCounters.WalBytesRead,
	}
	for k, v := range s.sealCounters.Truncated {
		out.Truncated[k] = v
	}
	return out
}

// countLostBigValues backs the seal_lost_big_values_total series (№7).
func (s *Store) countLostBigValues(n int64) {
	s.sealMu.Lock()
	defer s.sealMu.Unlock()
	s.sealCounters.LostBigValues += n
}

// countWalBytesRead accumulates the №9 calls.wal read gauge.
func (s *Store) countWalBytesRead(n int64) {
	s.sealMu.Lock()
	defer s.sealMu.Unlock()
	s.sealCounters.WalBytesRead += n
}

func (s *Store) countSeal(res SealResult) {
	s.sealMu.Lock()
	defer s.sealMu.Unlock()
	s.sealCounters.Rows += int64(res.Rows)
	s.sealCounters.Files += int64(len(res.Files))
	if s.sealCounters.Truncated == nil {
		s.sealCounters.Truncated = map[string]int64{}
	}
	for reason, n := range res.Truncated {
		s.sealCounters.Truncated[reason] += int64(n)
	}
}

// sealBeforeCommit is a test-only failpoint: when set, Seal calls it after the
// class files reach their sealed names but BEFORE the pass commits (№6 fault
// injection). Production leaves it nil.
var sealBeforeCommit func() error

// Seal materializes the parquet files of one (pod-restart, bucket)
// (01-write-contract.md §6.5): a segment-ordered walk assembles the per-call
// blobs, every classification is re-derived from calls.wal against the full
// dictionary (§5.6), and the rows land in up to five retention-class files
// named per §7. uploaded_at stays NULL until the Uploader confirms the PUT.
//
// The pass commits atomically (№6): every parquet_local row, every segment
// refcount pin, and the watermark land in ONE transaction (CommitSealPass). A
// crash before the commit leaves only orphan files that the retry regenerates
// under the same deterministic names; a crash after it re-seals nothing.
func (s *Store) Seal(ctx context.Context, key PodRestartKey, bucket int64) (SealResult, error) {
	// Two concurrent seals of one (pod-restart, bucket) — the №9 worker pool
	// racing the memory-pressure trigger — would race on NextParquetSeq and
	// double-seal the same rows; the second caller yields to the one running.
	if !s.tryLockSealPair(key.String(), bucket) {
		return SealResult{Truncated: map[string]int{}}, nil
	}
	defer s.unlockSealPair(key.String(), bucket)

	pr, ok := s.PodRestart(key)
	if !ok {
		return SealResult{}, errors.Errorf("unknown pod-restart %s", key)
	}

	watermark, err := s.db.SealWatermark(key.String(), bucket)
	if err != nil {
		return SealResult{}, err
	}
	idxRows, err := s.db.CallsForSeal(bucket, key.String(), watermark)
	if err != nil {
		return SealResult{}, err
	}
	if len(idxRows) == 0 {
		return SealResult{Truncated: map[string]int{}}, nil
	}

	// A live pod-restart still holds unflushed bytes in its gzip writers; the
	// walk below reads the segment files from disk.
	if !pr.Closed() {
		if err := pr.FlushSegments(); err != nil {
			return SealResult{}, err
		}
	}

	rows, err := s.loadSealRows(pr, idxRows)
	if err != nil {
		return SealResult{}, err
	}

	scratchDir := filepath.Join(pr.dir, "parquet-sealing",
		time.UnixMilli(s.cfg.BucketStartMs(bucket)).UTC().Format(sealedNameStamp))
	if err := os.MkdirAll(scratchDir, 0o755); err != nil {
		return SealResult{}, errors.Wrap(err, "create seal scratch dir")
	}

	if err := s.assembleBlobs(ctx, pr, rows, scratchDir); err != nil {
		return SealResult{}, err
	}
	s.resolveBigValues(ctx, pr, rows)

	res, commits, err := s.writeSealedFiles(pr, bucket, rows, scratchDir)
	if err != nil {
		return SealResult{}, err
	}

	// Advance the watermark to the first uncovered calls.wal offset: a late
	// Call appends past it and re-marks the bucket for a patch seal (§6.6).
	newWatermark := watermark
	for i := range rows {
		if off := rows[i].idx.CallsWalOffset + 1; off > newWatermark {
			newWatermark = off
		}
	}
	if sealBeforeCommit != nil {
		if err := sealBeforeCommit(); err != nil {
			return SealResult{}, err
		}
	}
	if err := s.db.CommitSealPass(key.String(), bucket, commits, newWatermark, time.Now().UnixMilli()); err != nil {
		return SealResult{}, err
	}
	for _, f := range res.Files {
		log.Info(ctx, "sealed %s: %d rows", f.S3Key, f.Rows)
	}

	s.countSeal(res)
	// A closed pod-restart's dictionary was reloaded for this pass only; drop
	// the lazy handle back to the WAL (№1).
	if pr.Closed() {
		pr.unloadDictionary()
	}
	return res, nil
}

// tryLockSealPair marks one (pod-restart, bucket) as sealing; false means
// another seal of the same pair is already running.
func (s *Store) tryLockSealPair(podRestart string, bucket int64) bool {
	pair := fmt.Sprintf("%s#%d", podRestart, bucket)
	s.sealPairMu.Lock()
	defer s.sealPairMu.Unlock()
	if s.sealingPairs == nil {
		s.sealingPairs = map[string]struct{}{}
	}
	if _, busy := s.sealingPairs[pair]; busy {
		return false
	}
	s.sealingPairs[pair] = struct{}{}
	return true
}

func (s *Store) unlockSealPair(podRestart string, bucket int64) {
	pair := fmt.Sprintf("%s#%d", podRestart, bucket)
	s.sealPairMu.Lock()
	delete(s.sealingPairs, pair)
	s.sealPairMu.Unlock()
}

// loadSealRows pairs each index row with its full calls.wal record. The WAL
// retains raw parameter dictionary ids, so §5.6 can re-derive error_flag here.
// Each record is fetched by its indexed offset via positioned reads (№9): a
// bucket's seal reads only its own records, not the whole WAL — the old
// os.ReadFile replay made a 12-bucket backlog re-read the file 12 times.
func (s *Store) loadSealRows(pr *PodRestart, idxRows []CallIndexRow) ([]sealRow, error) {
	f, err := os.Open(filepath.Join(pr.dir, "calls.wal"))
	if err != nil {
		return nil, errors.Wrap(err, "open calls.wal")
	}
	defer func() { _ = f.Close() }()

	rows := make([]sealRow, len(idxRows))
	var read int64
	defer func() { s.countWalBytesRead(read) }()
	for i := range idxRows {
		rows[i] = sealRow{idx: idxRows[i], srcSegs: map[segKey]struct{}{}, dictIds: map[int]struct{}{}}
		body, n, err := ReadWalRecordAt(f, idxRows[i].CallsWalOffset)
		read += n
		if err != nil {
			return nil, errors.Wrapf(err, "call index of %s references a calls.wal record recovery did not keep",
				idxRows[i].PodRestart)
		}
		if err := json.Unmarshal(body, &rows[i].wal); err != nil {
			return nil, errors.Wrapf(err, "decode calls.wal record at offset %d", idxRows[i].CallsWalOffset)
		}
	}
	return rows, nil
}

// assembleBlobs runs the segment-ordered walk of 01-write-contract.md §6.5:
// each trace segment is decompressed exactly once, and each decompressed chunk
// is routed to every call assembly that spans it. Peak memory is the blobs of
// calls open across the segment cursor; a blob past the spill threshold
// overflows to a temp file under parquet-sealing/.
func (s *Store) assembleBlobs(ctx context.Context, pr *PodRestart, rows []sealRow, scratchDir string) error {
	chains := pr.chunkSnapshot()
	timerStart := pr.TimerStartMs()

	// Catalog view of the segments: paths for the walk, existence for the
	// truncation reasons.
	segRows, err := s.db.Segments(pr.Key.String())
	if err != nil {
		return err
	}
	segByKey := make(map[segKey]SegmentRow, len(segRows))
	for _, sr := range segRows {
		segByKey[segKey{sr.Stream, sr.RollingSeq}] = sr
	}

	// Per-segment chunk lists in offset order, and the thread of every chunk:
	// the walk cursor moves forward only.
	type chunkPos struct {
		ref      ChunkRef
		threadId uint64
	}
	bySeg := map[int][]chunkPos{}
	chunkThread := map[[2]int64]uint64{}
	for threadId, refs := range chains {
		for _, ref := range refs {
			bySeg[ref.RollingSeq] = append(bySeg[ref.RollingSeq], chunkPos{ref: ref, threadId: threadId})
			chunkThread[[2]int64{int64(ref.RollingSeq), ref.Offset}] = threadId
		}
	}
	seqs := make([]int, 0, len(bySeg))
	for seq := range bySeg {
		chunks := bySeg[seq]
		sort.Slice(chunks, func(i, j int) bool { return chunks[i].ref.Offset < chunks[j].ref.Offset })
		seqs = append(seqs, seq)
	}
	sort.Ints(seqs)

	var blobPrefix [8]byte
	binary.BigEndian.PutUint64(blobPrefix[:], uint64(timerStart))

	// One assembly per row, keyed by its start chunk. A pointer that resolves
	// to no indexed chunk is truncated up front: the segment is gone
	// (disk_budget) or its chunk index was released (idle_timeout, §4.6).
	starters := map[[2]int64][]*assembly{}
	pending := 0
	for i := range rows {
		row := &rows[i]
		at := [2]int64{int64(row.idx.TraceFileIndex), int64(row.idx.BufferOffset)}
		threadId, ok := chunkThread[at]
		if !ok {
			seg, catalogued := segByKey[segKey{StreamTrace, row.idx.TraceFileIndex}]
			if !catalogued || seg.Status == "evicted" || !fileExists(seg.Path) {
				row.truncate(TruncDiskBudget)
			} else {
				row.truncate(TruncIdleTimeout)
			}
			continue
		}
		row.blob = newBlobBuffer(scratchDir, s.cfg.SealSpillBytes, blobPrefix[:])
		starters[at] = append(starters[at], &assembly{row: row, threadId: threadId})
		pending++
	}

	active := map[uint64][]*assembly{}
	activeCount := 0
	fail := func(a *assembly, reason string) {
		a.done = true
		a.row.truncate(reason)
	}

	for _, seq := range seqs {
		if err := ctx.Err(); err != nil {
			return err
		}
		chunks := bySeg[seq]
		startsHere := false
		for _, c := range chunks {
			if _, ok := starters[[2]int64{int64(seq), c.ref.Offset}]; ok {
				startsHere = true
				break
			}
		}
		if activeCount == 0 && !startsHere {
			continue // nothing spans this segment; leave it compressed
		}
		if pending == 0 && activeCount == 0 {
			break
		}

		segRow, ok := segByKey[segKey{StreamTrace, seq}]
		var reader *segmentReader
		var openErr error
		if ok {
			reader, openErr = openSegmentReader(segRow.Path)
		}
		if !ok || openErr != nil {
			// The segment was evicted under the disk budget (§4.6): every call
			// that starts in it or is open across it loses its blob.
			if openErr != nil {
				log.Warning(ctx, "seal: trace segment %d of %v is unreadable: %v", seq, pr.Key, openErr)
			}
			for _, c := range chunks {
				for _, a := range starters[[2]int64{int64(seq), c.ref.Offset}] {
					fail(a, TruncDiskBudget)
					pending--
				}
				delete(starters, [2]int64{int64(seq), c.ref.Offset})
				activeCount -= truncateActive(active, c.threadId, fail)
			}
			continue
		}

		pos := int64(0)
		segTorn := false
		for _, c := range chunks {
			starts := starters[[2]int64{int64(seq), c.ref.Offset}]
			consumers := active[c.threadId]
			if len(starts) == 0 && len(consumers) == 0 {
				continue
			}

			var data []byte
			if !segTorn {
				if _, err := io.CopyN(io.Discard, reader, c.ref.Offset-pos); err == nil {
					data = make([]byte, c.ref.Length)
					if _, err := io.ReadFull(reader, data); err != nil {
						data = nil
					}
				}
				pos = c.ref.Offset + int64(c.ref.Length)
			}
			if data == nil {
				// A torn tail (crash mid-flush): the rest of the segment is
				// unreadable, but its chunks were indexed, so the affected
				// calls are truncated like an eviction.
				segTorn = true
				for _, a := range starts {
					fail(a, TruncDiskBudget)
					pending--
				}
				delete(starters, [2]int64{int64(seq), c.ref.Offset})
				activeCount -= truncateActive(active, c.threadId, fail)
				continue
			}

			next := make([]*assembly, 0, len(consumers)+len(starts))
			for _, a := range consumers {
				s.consumeChunk(ctx, pr, a, seq, data)
				if a.done {
					activeCount--
				} else {
					next = append(next, a)
				}
			}
			for _, a := range starts {
				pending--
				s.consumeChunk(ctx, pr, a, seq, data)
				if !a.done {
					next = append(next, a)
					activeCount++
				}
			}
			delete(starters, [2]int64{int64(seq), c.ref.Offset})
			active[c.threadId] = next
		}
		_ = reader.Close()
	}

	// A chain that ran out of chunks before the depth-0 exit lost its tail —
	// the trailing segments or chunks are gone (§4.6).
	for _, assemblies := range active {
		for _, a := range assemblies {
			if !a.done {
				fail(a, TruncDiskBudget)
			}
		}
	}
	return nil
}

// consumeChunk appends one full chunk to the assembly's blob (§4.5: blobs
// carry whole chunks; the reader trims tail/head noise) and advances the
// call-depth walk to detect the depth-0 exit.
func (s *Store) consumeChunk(ctx context.Context, pr *PodRestart, a *assembly, seq int, data []byte) {
	if err := a.row.blob.Append(data); err != nil {
		log.Error(ctx, err, "seal: spill blob of call %v/%d/%d/%d", pr.Key,
			a.row.idx.TraceFileIndex, a.row.idx.BufferOffset, a.row.idx.RecordIndex)
		a.done = true
		a.row.truncate(TruncMemPressure)
		return
	}
	a.row.srcSegs[segKey{StreamTrace, seq}] = struct{}{}

	recordIndex := a.row.idx.RecordIndex
	timerStart := pr.TimerStartMs()
	// trackTime widens the row's event-time span (the suspend_json window).
	trackTime := func(elapsedMs int64) {
		at := timerStart + elapsedMs
		if !a.row.hasBlobTime || at < a.row.blobTimeMinMs {
			a.row.blobTimeMinMs = at
		}
		if !a.row.hasBlobTime || at > a.row.blobTimeMaxMs {
			a.row.blobTimeMaxMs = at
		}
		a.row.hasBlobTime = true
	}
	badPointer := false
	_, _, err := ParseChunk(data, func(index int, ev TraceEvent, elapsedMs int64) bool {
		if !a.started {
			if index < recordIndex {
				return true // tail noise: the previous root call of this thread
			}
			if ev.Kind != TraceEnter {
				badPointer = true
				return false
			}
			a.started = true
			a.depth = 1
			// The root ENTER's method id opens the row's dictionary subset
			// (dict_words_json): every id the reader will resolve is recorded
			// here, during the one pass that already parses the events.
			a.row.dictIds[ev.TagId] = struct{}{}
			trackTime(elapsedMs)
			return true
		}
		trackTime(elapsedMs)
		switch ev.Kind {
		case TraceEnter:
			a.depth++
			a.row.dictIds[ev.TagId] = struct{}{}
		case TraceExit:
			a.depth--
			if a.depth == 0 {
				a.done = true
				return false
			}
		case TraceTag:
			a.row.dictIds[ev.TagId] = struct{}{}
			// The blob points into the external value streams; they must
			// survive with it (03-lifecycle.md §3.2), so they join the refcount.
			// The full reference is kept too: the seal resolves it into
			// big_params_json so the cold tier can inline the value (§4.4).
			switch int(ev.ParamType) {
			case pipe.ParamBigDedup:
				a.row.srcSegs[segKey{StreamSql, ev.BigSeq}] = struct{}{}
				a.row.bigRefs = append(a.row.bigRefs, ValueRef{StreamSql, ev.BigSeq, int64(ev.BigOffset)})
			case pipe.ParamBig:
				a.row.srcSegs[segKey{StreamXml, ev.BigSeq}] = struct{}{}
				a.row.bigRefs = append(a.row.bigRefs, ValueRef{StreamXml, ev.BigSeq, int64(ev.BigOffset)})
			}
		}
		return true
	})
	if badPointer || err != nil {
		if badPointer {
			log.Warning(ctx, "seal: record_index %d of call %v/%d/%d does not land on an ENTER",
				recordIndex, pr.Key, a.row.idx.TraceFileIndex, a.row.idx.BufferOffset)
		} else {
			log.Warning(ctx, "seal: chunk of call %v/%d/%d/%d does not parse: %v", pr.Key,
				a.row.idx.TraceFileIndex, a.row.idx.BufferOffset, recordIndex, err)
		}
		a.done = true
		a.row.truncate(TruncIdleTimeout)
	}
}

// resolveBigValues inlines the big-parameter values the assembled blobs
// reference (01-write-contract.md §6.5 step 3): every referenced sql / xml
// segment is read once across the whole pass, and each row keeps its own
// values keyed by the reference text. A reference that fails to resolve —
// its value segment was evicted or lost its tail — is a permanent loss for
// the cold tier (the segments never reach S3), so the row seals truncated
// with disk_budget and the loss lands on seal_lost_big_values_total (№7):
// never a silently-empty big_params_json.
func (s *Store) resolveBigValues(ctx context.Context, pr *PodRestart, rows []sealRow) {
	var refs []ValueRef
	for i := range rows {
		if rows[i].truncated == "" {
			refs = append(refs, rows[i].bigRefs...)
		}
	}
	if len(refs) == 0 {
		return
	}
	values := pr.readBigValues(ctx, refs)
	var lost int64
	for i := range rows {
		row := &rows[i]
		if row.truncated != "" || len(row.bigRefs) == 0 {
			continue
		}
		resolved := map[string]string{}
		missing := 0
		for _, ref := range row.bigRefs {
			if v, ok := values[ref]; ok {
				resolved[ref.String()] = v
			} else {
				missing++
			}
		}
		if missing > 0 {
			lost += int64(missing)
			log.Warning(ctx, "seal: call %v/%d/%d/%d lost %d big-parameter values to eviction; sealing it truncated",
				pr.Key, row.idx.TraceFileIndex, row.idx.BufferOffset, row.idx.RecordIndex, missing)
			row.truncate(TruncDiskBudget)
			continue
		}
		if len(resolved) > 0 {
			row.bigValues = resolved
		}
	}
	if lost > 0 {
		s.countLostBigValues(lost)
	}
}

// truncateActive fails every open assembly of a thread and returns how many
// were dropped from the active set.
func truncateActive(active map[uint64][]*assembly, threadId uint64, fail func(*assembly, string)) int {
	assemblies := active[threadId]
	for _, a := range assemblies {
		fail(a, TruncDiskBudget)
	}
	delete(active, threadId)
	return len(assemblies)
}

// writeSealedFiles renders the CallV2 rows, routes them into per-class writers
// (§6.4), and moves each finished file from scratch to its sealed name (§6.2
// steps 1-2; upload is the S3 task). Nothing is recorded in the catalog here:
// the caller commits every file plus the watermark in one transaction (№6).
func (s *Store) writeSealedFiles(pr *PodRestart, bucket int64, rows []sealRow, scratchDir string) (SealResult, []sealCommit, error) {
	res := SealResult{Truncated: map[string]int{}}

	dict := pr.Dictionary()
	redId, hasRed := pr.DictId(errorMarkerParam)
	pauses, err := readSuspendWal(pr)
	if err != nil {
		return res, nil, err
	}

	// CallsForSeal orders by (ts_ms DESC, pk ASC) — the §5.2 row order — and
	// the per-class partition below keeps it.
	byClass := map[string][]*sealRow{}
	for i := range rows {
		row := &rows[i]
		errorFlag := hasRed && paramsHave(row.wal.Call.Params, redId)
		class := s.cfg.RetentionClass(time.Duration(row.wal.Call.Duration)*time.Millisecond, errorFlag)
		if _, ok := dict[row.wal.Call.Method]; !ok {
			// §5.1: an unresolvable method leaves the blob undecodable.
			row.truncate(TruncDictMiss)
		}
		byClass[class] = append(byClass[class], row)
	}

	var commits []sealCommit
	// The class list derives from the shared tier table (№10); classes with
	// no rows in this bucket (corrupted stays reserved-empty, §5.6) open no
	// writer.
	for _, class := range model.RetentionClasses {
		classRows := byClass[class]
		if len(classRows) == 0 {
			continue
		}
		file, commit, err := s.writeClassFile(pr, bucket, class, classRows, scratchDir, dict, redId, hasRed, pauses)
		if err != nil {
			return res, nil, err
		}
		res.Files = append(res.Files, file)
		res.Rows += file.Rows
		commits = append(commits, commit)
		for _, row := range classRows {
			if row.truncated != "" {
				res.Truncated[row.truncated]++
			}
		}
	}
	return res, commits, nil
}

// writeClassFile writes one retention class of the bucket: scratch parquet,
// fsync, then the atomic rename to the sealed name (plus a directory fsync,
// №27 — a torn file must never reach S3 to live out its whole TTL there). The
// catalog record is returned to the caller for the pass-level commit (№6).
func (s *Store) writeClassFile(pr *PodRestart, bucket int64, class string,
	classRows []*sealRow, scratchDir string, dict map[int]string, redId int, hasRed bool,
	pauses []SuspendPause) (SealedFile, sealCommit, error) {

	key := pr.Key
	seq, err := s.db.NextParquetSeq(key.String(), s.cfg.BucketStartMs(bucket), class)
	if err != nil {
		return SealedFile{}, sealCommit{}, err
	}

	scratch := filepath.Join(scratchDir, fmt.Sprintf("%s-%d.parquet", class, seq))
	fw, err := os.Create(scratch)
	if err != nil {
		return SealedFile{}, sealCommit{}, errors.Wrap(err, "create seal scratch file")
	}
	// §5.2: every file is ZSTD. The schema-version stamp lets a future reader
	// branch on a non-additive CallV2 change; additive evolution needs no bump
	// (the reader null-fills by column name). Page bounds are skipped for the
	// blob-sized columns — their min/max would copy blob prefixes into the
	// footer for a column nobody range-prunes on.
	pw := parquet.NewGenericWriter[storageparquet.CallV2](fw,
		storageparquet.CallV2WriterOptions()...)

	timeMin, timeMax := int64(0), int64(0)
	segRowCounts := map[segKey]int{}
	for i, row := range classRows {
		v, err := s.renderRow(pr, row, class, dict, redId, hasRed, pauses)
		if err != nil {
			_ = fw.Close()
			return SealedFile{}, sealCommit{}, err
		}
		if _, err := pw.Write([]storageparquet.CallV2{*v}); err != nil {
			_ = fw.Close()
			return SealedFile{}, sealCommit{}, errors.Wrap(err, "write parquet row")
		}
		if i == 0 || row.idx.TsMs < timeMin {
			timeMin = row.idx.TsMs
		}
		if i == 0 || row.idx.TsMs > timeMax {
			timeMax = row.idx.TsMs
		}
		for sk := range row.srcSegs {
			segRowCounts[sk]++
		}
	}
	if err := pw.Close(); err != nil {
		_ = fw.Close()
		return SealedFile{}, sealCommit{}, errors.Wrap(err, "finish parquet file")
	}
	// §6.2 step 1 promises "fully on disk" before the rename; without the
	// fsync a power loss can leave a torn file under its final name, which the
	// uploader would happily PUT (№27).
	if err := fw.Sync(); err != nil {
		_ = fw.Close()
		return SealedFile{}, sealCommit{}, errors.Wrap(err, "fsync parquet file")
	}
	if err := fw.Close(); err != nil {
		return SealedFile{}, sealCommit{}, errors.Wrap(err, "close parquet file")
	}
	for _, row := range classRows {
		row.freeBlob()
	}

	// The sealed name is the S3 object key (01 §7); the local copy mirrors it
	// under DataDir so the upload task reads parquet_local.s3_key verbatim.
	bucketStart := time.UnixMilli(s.cfg.BucketStartMs(bucket)).UTC()
	baseName := fmt.Sprintf("%s-%s-%s-%s-%s-%d.parquet",
		s.cfg.Replica, PodRestartHash(key),
		bucketStart.Format(sealedNameStamp),
		time.UnixMilli(timeMin).UTC().Format(sealedNameStamp),
		time.UnixMilli(timeMax).UTC().Format(sealedNameStamp),
		seq)
	s3Key := path.Join("parquet/v1", class, bucketStart.Format("2006/01/02/15"), baseName)
	sealedPath := filepath.Join(s.cfg.DataDir, filepath.FromSlash(s3Key))
	if err := os.MkdirAll(filepath.Dir(sealedPath), 0o755); err != nil {
		return SealedFile{}, sealCommit{}, errors.Wrap(err, "create sealed parquet dir")
	}
	// A crashed pass may have left an orphan under the same deterministic
	// name (rename done, commit not); the rename replaces it.
	if err := os.Rename(scratch, sealedPath); err != nil {
		return SealedFile{}, sealCommit{}, errors.Wrap(err, "move sealed parquet")
	}
	// №27 second half: make the rename itself durable before the catalog (and
	// later the uploader) can see the file.
	if err := syncDir(filepath.Dir(sealedPath)); err != nil {
		return SealedFile{}, sealCommit{}, err
	}
	info, err := os.Stat(sealedPath)
	if err != nil {
		return SealedFile{}, sealCommit{}, errors.Wrap(err, "stat sealed parquet")
	}

	commit := sealCommit{
		row: parquetLocalRow{
			Path:           sealedPath,
			PodRestart:     key.String(),
			TimeBucketMs:   s.cfg.BucketStartMs(bucket),
			RetentionClass: class,
			Seq:            seq,
			RowCount:       len(classRows),
			TimeMinMs:      timeMin,
			TimeMaxMs:      timeMax,
			FileSize:       info.Size(),
			SealedAtMs:     time.Now().UnixMilli(),
			S3Key:          s3Key,
		},
		segRows: segRowCounts,
	}
	return SealedFile{
		Path:           sealedPath,
		S3Key:          s3Key,
		RetentionClass: class,
		Seq:            seq,
		Rows:           len(classRows),
		TimeMinMs:      timeMin,
		TimeMaxMs:      timeMax,
	}, commit, nil
}

// syncDir fsyncs a directory so a just-renamed entry survives a power loss.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return errors.Wrap(err, "open dir for fsync")
	}
	defer func() { _ = d.Close() }()
	return errors.Wrap(d.Sync(), "fsync dir")
}

// renderRow builds the CallV2 row: filter columns from the index row, the
// remaining columns from calls.wal, suspend_ms from the pause intersection
// (§5.1 step 4), and every classification re-derived against the full
// dictionary (§5.6).
func (s *Store) renderRow(pr *PodRestart, row *sealRow, class string, dict map[int]string,
	redId int, hasRed bool, pauses []SuspendPause) (*storageparquet.CallV2, error) {

	call := row.wal.Call
	key := pr.Key

	method, ok := dict[call.Method]
	if !ok {
		method = fmt.Sprintf("#%d", call.Method)
	}
	params := storageparquet.Parameters{}
	for id, values := range call.Params {
		name, ok := dict[id]
		if !ok {
			name = fmt.Sprintf("#%d", id)
		}
		params.AddVal(name, values...)
	}

	v := &storageparquet.CallV2{
		TsMs:           row.idx.TsMs,
		PodId:          fmt.Sprintf("%s/%s/%s", key.Namespace, key.Service, key.PodName),
		RestartTimeMs:  key.RestartTimeMs,
		TraceFileIndex: int32(call.TraceFileIndex),
		BufferOffset:   int32(call.BufferOffset),
		RecordIndex:    int32(call.RecordIndex),
		ThreadName:     call.ThreadName,
		Namespace:      key.Namespace,
		ServiceName:    key.Service,
		PodName:        key.PodName,
		Method:         method,
		// The int32 duration columns clamp instead of wrapping: a >24.8-day
		// call must saturate at MaxInt32, not go negative.
		DurationMs:     clampInt32(int64(call.Duration)),
		CpuTimeMs:      call.CpuTime,
		WaitTimeMs:     call.WaitTime,
		MemoryUsed:     call.MemoryUsed,
		QueueWaitMs:    clampInt32(int64(call.QueueWaitDuration)),
		SuspendMs:      clampInt32(int64(suspendOverlapMs(pauses, row.idx.TsMs, call.Duration))),
		ChildCalls:     clampInt32(int64(call.Calls)),
		Transactions:   clampInt32(int64(call.Transactions)),
		LogsGenerated:  int64(call.LogsGenerated),
		LogsWritten:    int64(call.LogsWritten),
		FileRead:       int64(call.FileRead),
		FileWritten:    int64(call.FileWritten),
		NetRead:        int64(call.NetRead),
		NetWritten:     int64(call.NetWritten),
		ErrorFlag:      hasRed && paramsHave(call.Params, redId),
		RetentionClass: class,
		Params:         params,
	}
	if row.truncated != "" {
		reason := row.truncated
		v.TruncatedReason = &reason
		return v, nil
	}
	blob, err := row.blob.Bytes()
	if err != nil {
		return nil, errors.Wrap(err, "read assembled blob")
	}
	v.TraceBlob = blob
	if len(row.bigValues) > 0 {
		encoded, err := json.Marshal(row.bigValues)
		if err != nil {
			return nil, errors.Wrap(err, "encode big_params_json")
		}
		bigStr := string(encoded)
		v.BigParamsJson = &bigStr
	}
	// The row is self-contained (№3, №23): the dictionary subset its blob
	// references and the pauses overlapping its window ride in the row, so
	// the cold /tree path never needs a snapshot object. An id the full
	// dictionary cannot resolve is left out — the reader renders "#<id>",
	// matching the params column above.
	subset := make(map[int]string, len(row.dictIds))
	for id := range row.dictIds {
		if word, ok := dict[id]; ok {
			subset[id] = word
		}
	}
	if v.DictWordsJson, err = storageparquet.EncodeDictWords(subset); err != nil {
		return nil, err
	}
	// The window is the blob's own event-time span (§4.2) — the axis the
	// tree renders node windows on — so every pause any node can intersect
	// is inlined, whatever the calls-stream epoch says.
	if row.hasBlobTime {
		var overlapping []storageparquet.SuspendEvent
		for _, p := range pauses {
			if p.TimeMs-int64(p.DurationMs) <= row.blobTimeMaxMs && p.TimeMs >= row.blobTimeMinMs {
				overlapping = append(overlapping, storageparquet.SuspendEvent{
					EndMs: p.TimeMs, DurationMs: int64(p.DurationMs)})
			}
		}
		if v.SuspendJson, err = storageparquet.EncodeSuspend(overlapping); err != nil {
			return nil, err
		}
	}
	return v, nil
}

// truncate drops the row's blob and records the reason (§5.2). A NULL blob
// sources no segment, resolves no values, and references no dictionary ids,
// so the row's refcount contribution, its big-parameter references, and its
// dictionary subset are cleared too.
func (r *sealRow) truncate(reason string) {
	if r.truncated == "" {
		r.truncated = reason
	}
	r.srcSegs = map[segKey]struct{}{}
	r.bigRefs = nil
	r.bigValues = nil
	r.dictIds = map[int]struct{}{}
	r.freeBlob()
}

func (r *sealRow) freeBlob() {
	if r.blob != nil {
		r.blob.Free()
		r.blob = nil
	}
}

func paramsHave(params map[int][]string, id int) bool {
	_, ok := params[id]
	return ok
}

// clampInt32 saturates instead of wrapping when a 64-bit counter or duration
// leaves the int32 parquet column's range.
func clampInt32(v int64) int32 {
	switch {
	case v > math.MaxInt32:
		return math.MaxInt32
	case v < math.MinInt32:
		return math.MinInt32
	default:
		return int32(v)
	}
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// segmentReader is a gzip cursor over one hot-store segment; Close releases
// both the gzip state and the underlying file.
type segmentReader struct {
	f  *os.File
	gz *gzip.Reader
}

func (r *segmentReader) Read(p []byte) (int, error) { return r.gz.Read(p) }

func (r *segmentReader) Close() error {
	gzErr := r.gz.Close()
	fErr := r.f.Close()
	if gzErr != nil {
		return gzErr
	}
	return fErr
}

func openSegmentReader(path string) (*segmentReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	gz, err := gzip.NewReader(f)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return &segmentReader{f: f, gz: gz}, nil
}

// readSuspendWal decodes the pod-restart's stop-the-world pauses (§3.6).
func readSuspendWal(pr *PodRestart) ([]SuspendPause, error) {
	var pauses []SuspendPause
	_, err := replayIfPresent(filepath.Join(pr.dir, "suspend.wal"), func(_ int64, body []byte) error {
		var rec SuspendPause
		if err := json.Unmarshal(body, &rec); err != nil {
			return errors.Wrap(err, "decode suspend.wal record")
		}
		pauses = append(pauses, rec)
		return nil
	})
	return normalizeSuspendPauses(pauses), err
}

// normalizeSuspendPauses sorts the timeline and merges overlapping or touching
// pauses, so suspendOverlapMs never counts the same wall-clock millisecond
// twice. Duplicate or overlapping suspend.wal records (recovery replay, agent
// hiccups) would otherwise inflate suspend_ms; the tree path folds them the
// same way (calltree.normalizeSuspend). The input slice is not mutated.
//
// SuspendPause.TimeMs is the pause END, the wall-clock instant the agent
// timestamped after detecting the delay (TimerCache dates[pos]=now; the
// reference SuspendLog builds start = date − delay). The pause therefore spans
// [TimeMs − DurationMs, TimeMs]; the normalizer keys the merge off that start
// and preserves the (end, duration) representation so downstream still reads
// TimeMs as the end (№4).
func normalizeSuspendPauses(pauses []SuspendPause) []SuspendPause {
	if len(pauses) == 0 {
		return nil
	}
	sorted := append([]SuspendPause(nil), pauses...)
	start := func(p SuspendPause) int64 { return p.TimeMs - int64(p.DurationMs) }
	// Sort by start so a linear merge sees the pauses in interval order.
	sort.Slice(sorted, func(i, j int) bool { return start(sorted[i]) < start(sorted[j]) })
	out := sorted[:1]
	for _, p := range sorted[1:] {
		last := &out[len(out)-1]
		if start(p) <= last.TimeMs { // p starts before or as the previous pause ends
			if p.TimeMs > last.TimeMs { // extend the merged end, keeping the earlier start
				last.DurationMs = int(p.TimeMs - start(*last))
				last.TimeMs = p.TimeMs
			}
			continue
		}
		out = append(out, p)
	}
	return out
}

// suspendOverlapMs sums the intersection of the call interval
// [tsMs, tsMs+durationMs] with the pause intervals (§5.1 step 4). Each pause
// spans [TimeMs − DurationMs, TimeMs] (TimeMs is the pause end; see
// normalizeSuspendPauses). It assumes a normalized (sorted, non-overlapping)
// timeline so overlapping pauses are not double-counted.
func suspendOverlapMs(pauses []SuspendPause, tsMs int64, durationMs int) int {
	callEnd := tsMs + int64(durationMs)
	total := int64(0)
	for _, p := range pauses {
		lo, hi := tsMs, callEnd
		if start := p.TimeMs - int64(p.DurationMs); start > lo {
			lo = start
		}
		if p.TimeMs < hi {
			hi = p.TimeMs
		}
		if hi > lo {
			total += hi - lo
		}
	}
	return int(total)
}

// SealDue runs a seal pass for every (pod-restart, bucket) whose bucket ended
// more than TimeBucketGrace ago and whose call index holds rows past the seal
// watermark (§6.1 trigger 1; the late-data dirty flag re-uses the same check
// because a late Call raises the partition's max calls_wal_offset). Under the
// №2 backpressure the due pairs are still counted — the seal_queue_depth
// gauge must grow while sealing pauses — but no seal runs: the calls stay in
// the WALs, segments, and partitions instead of becoming more pending parquet.
func (s *Store) SealDue(ctx context.Context, nowMs int64) (int, error) {
	if err := s.refreshBackpressure(ctx); err != nil {
		return 0, err
	}
	buckets, err := s.db.Buckets()
	if err != nil {
		return 0, err
	}
	type duePair struct {
		key    PodRestartKey
		bucket int64
	}
	var due []duePair
	for _, bucket := range buckets {
		dueMs := s.cfg.BucketStartMs(bucket) + s.cfg.TimeBucket.Milliseconds() + s.cfg.TimeBucketGrace.Milliseconds()
		if dueMs > nowMs {
			continue
		}
		maxOffsets, err := s.db.MaxWalOffsets(bucket)
		if err != nil {
			return 0, err
		}
		for podRestart, maxOffset := range maxOffsets {
			watermark, err := s.db.SealWatermark(podRestart, bucket)
			if err != nil {
				return 0, err
			}
			if maxOffset < watermark {
				continue
			}
			key, err := ParsePodRestartKey(podRestart)
			if err != nil {
				log.Error(ctx, err, "seal loop: skip unparseable pod_restart %q", podRestart)
				continue
			}
			due = append(due, duePair{key: key, bucket: bucket})
		}
	}
	s.sealQueueDepth.Store(int64(len(due)))
	if len(due) == 0 {
		return 0, nil
	}
	if s.sealPaused.Load() {
		log.Warning(ctx, "seal loop paused by backpressure: %d buckets due; their calls stay in the WALs and segments", len(due))
		return 0, nil
	}

	// The due pairs run over a worker pool of PROFILER_SEAL_CONCURRENCY (§6.1,
	// №9). A pair that fails — a poisoned bucket whose index outran its WAL —
	// is skipped with a metric instead of aborting the pass (№8): the other
	// buckets keep sealing, and the contiguity barrier keeps moving. The №2
	// gate stays honoured: once it trips mid-pass, workers stop taking pairs
	// and in-flight seals finish.
	workers := s.cfg.SealConcurrency
	if workers > len(due) {
		workers = len(due)
	}
	var (
		sealed atomic.Int64
		stop   atomic.Bool
		errMu  sync.Mutex
		errs   []error
		jobs   = make(chan duePair)
		wg     sync.WaitGroup
	)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for d := range jobs {
				if stop.Load() || ctx.Err() != nil {
					continue // drain: the pass is pausing or cancelled
				}
				if _, err := s.Seal(ctx, d.key, d.bucket); err != nil {
					s.sealSkippedBuckets.Add(1)
					log.Error(ctx, err, "seal loop: skipping poisoned bucket %d of %s this pass", d.bucket, d.key)
					errMu.Lock()
					errs = append(errs, errors.Wrapf(err, "seal %s bucket %d", d.key, d.bucket))
					errMu.Unlock()
				} else {
					sealed.Add(1)
				}
				s.sealQueueDepth.Add(-1)
				// Every seal grows the pending backlog; stop mid-pass the
				// moment the budget trips rather than one whole pass later.
				if err := s.refreshBackpressure(ctx); err != nil {
					errMu.Lock()
					errs = append(errs, err)
					errMu.Unlock()
					stop.Store(true)
					continue
				}
				if s.sealPaused.Load() && !stop.Swap(true) {
					log.Warning(ctx, "seal loop paused mid-pass by backpressure after %d of %d due buckets",
						sealed.Load(), len(due))
				}
			}
		}()
	}
	for _, d := range due {
		jobs <- d
	}
	close(jobs)
	wg.Wait()
	return int(sealed.Load()), stderrors.Join(errs...)
}

// SealSkippedBuckets reports how many (pod-restart, bucket) seals were skipped
// after a failure (№8) — the seal_skipped_buckets_total seam. The pair is
// retried on every pass, so a transient failure recovers on the next tick and
// a sustained rate marks a genuinely poisoned bucket.
func (s *Store) SealSkippedBuckets() int64 { return s.sealSkippedBuckets.Load() }

// sealOldestUnsealedBucket implements the §6.1 memory-pressure trigger
// (trigger 3): under RAM-budget pressure the janitor seals the OLDEST bucket
// that still holds unsealed calls, regardless of the bucket-end grace, so a
// hot bucket's chunk-index references and seal buffers can drain — after it, a
// closed pod-restart with no unsealed calls left releases its chunk index. The
// №2 gate stays honoured: a paused seal stays paused, whatever the RAM says
// (more sealing means more pending parquet, the very thing the gate bounds).
func (s *Store) sealOldestUnsealedBucket(ctx context.Context) (int, error) {
	if s.sealPaused.Load() {
		return 0, nil
	}
	buckets, err := s.db.Buckets()
	if err != nil {
		return 0, err
	}
	for _, bucket := range buckets {
		maxOffsets, err := s.db.MaxWalOffsets(bucket)
		if err != nil {
			return 0, err
		}
		sealedPods := 0
		for podRestart, maxOffset := range maxOffsets {
			watermark, err := s.db.SealWatermark(podRestart, bucket)
			if err != nil {
				return 0, err
			}
			if maxOffset < watermark {
				continue
			}
			key, err := ParsePodRestartKey(podRestart)
			if err != nil {
				log.Error(ctx, err, "mem-pressure seal: skip unparseable pod_restart %q", podRestart)
				continue
			}
			if _, err := s.Seal(ctx, key, bucket); err != nil {
				s.sealSkippedBuckets.Add(1)
				log.Error(ctx, err, "mem-pressure seal: skipping poisoned bucket %d of %s", bucket, key)
				continue
			}
			sealedPods++
		}
		if sealedPods > 0 {
			log.Warning(ctx, "mem budget: early-sealed bucket %d (%d pod-restarts) to relieve RAM (01 §6.1 trigger 3)", bucket, sealedPods)
			return sealedPods, nil
		}
	}
	return 0, nil
}

// RunSealLoop polls SealDue until the context ends: the §6.1 bucket-end
// trigger; the memory-pressure trigger rides the janitor's mem-budget step.
func (s *Store) RunSealLoop(ctx context.Context, interval time.Duration) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if _, err := s.SealDue(ctx, time.Now().UnixMilli()); err != nil {
				s.sealLoopErrors.Add(1)
				log.Error(ctx, err, "seal loop pass failed")
			}
		}
	}
}

// ParsePodRestartKey inverts PodRestartKey.String. Kubernetes names cannot
// contain '/', so the split is unambiguous.
func ParsePodRestartKey(s string) (PodRestartKey, error) {
	parts := strings.SplitN(s, "/", 4)
	if len(parts) != 4 {
		return PodRestartKey{}, errors.Errorf("pod_restart %q: expected ns/service/pod/restartMs", s)
	}
	ms, err := strconv.ParseInt(parts[3], 10, 64)
	if err != nil {
		return PodRestartKey{}, errors.Wrapf(err, "pod_restart %q: bad restart time", s)
	}
	return PodRestartKey{Namespace: parts[0], Service: parts[1], PodName: parts[2], RestartTimeMs: ms}, nil
}
