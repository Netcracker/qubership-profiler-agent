package hotstore

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/protocol/data"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	"github.com/pkg/errors"
)

// errorMarkerParam is the indexed parameter the agent's ExceptionLogger
// records on an errored call; its presence in Call.Params is the error_flag
// source (01-write-contract.md §5.6).
const errorMarkerParam = "call.red"

type (
	// PodRestartKey identifies one agent TCP connection: the pod triple plus
	// the restart time the collector stamped at accept (01 §1 V4).
	PodRestartKey struct {
		Namespace     string
		Service       string
		PodName       string
		RestartTimeMs int64
	}

	// ChunkRef locates one logical trace chunk inside a hot-store segment: the
	// in-RAM chunk_index entry of 01-write-contract.md §4.3.
	ChunkRef struct {
		RollingSeq int   // agent stream-file index = segment file name
		Offset     int64 // logical (uncompressed) offset of the 16-byte chunk header
		Length     int   // chunk length in bytes, header included
		StartMs    int64 // absolute chunk start from the header
	}

	// Segment is an open hot-store segment plus the write-path bookkeeping the
	// catalog row needs at finalize.
	Segment struct {
		w         *SegmentWriter
		timeMinMs *int64
		timeMaxMs *int64
	}

	// ParamRecord is one params-stream record persisted to params.wal.
	ParamRecord struct {
		Name      string `json:"name"`
		IsIndex   bool   `json:"is_index"`
		IsList    bool   `json:"is_list"`
		Order     int    `json:"order"`
		Signature string `json:"signature,omitempty"`
	}

	// SuspendPause is one suspend.wal body: an absolute stop-the-world pause.
	SuspendPause struct {
		TimeMs     int64 `json:"time_ms"`
		DurationMs int   `json:"duration_ms"`
	}

	// CallWalRecord is one calls.wal body. The wire record is not
	// self-contained (delta times, per-file thread-name table), so the WAL
	// stores the decoded record with its absolute start time; see the Stage 1
	// decisions log.
	CallWalRecord struct {
		TsMs int64     `json:"ts_ms"`
		Call data.Call `json:"call"`
	}

	// PodRestart holds the live state of one pod-restart: WAL writers, the
	// in-RAM dictionary, and the per-thread chunk index.
	PodRestart struct {
		Key PodRestartKey

		store *Store
		dir   string

		mu           sync.Mutex
		closed       bool // no new writes accepted
		finalized    bool // WAL footers written, catalog row closed
		dict         map[int]string
		dictIds      map[string]int
		nextWordId   int
		timerStartMs int64
		chunks       map[uint64][]ChunkRef
		segments     map[*Segment]struct{}
		// pauses mirrors suspend.wal in RAM so indexCall can attribute
		// suspend_ms without a per-call WAL read. The index value is
		// provisional — a pause that arrives after the call's insert is
		// missed; the seal pass re-derives from suspend.wal (01 §5.1 step 4).
		pauses []SuspendPause

		dictWal    *Wal
		paramsWal  *Wal
		suspendWal *Wal
		callsWal   *Wal
	}

	// Store owns the PV: the exclusive lock, the SQLite metadata, and the
	// per-pod-restart state.
	Store struct {
		cfg  Config
		db   *metaDb
		lock *os.File

		mu   sync.Mutex
		pods map[string]*PodRestart

		sealMu       sync.Mutex
		sealCounters SealCounters

		janitorMu       sync.Mutex
		janitorCounters JanitorStats
		// Gauges measured by the janitor pass; their freshness is the janitor
		// interval, which is fine for a scrape and avoids stat-walking the PV
		// or locking every pod-restart on each /metrics request.
		segmentsDiskBytes atomic.Int64
		evictedChunkRefs  atomic.Int64
	}
)

// String renders the scalar pod_restart key used across the SQLite tables.
func (k PodRestartKey) String() string {
	return fmt.Sprintf("%s/%s/%s/%d", k.Namespace, k.Service, k.PodName, k.RestartTimeMs)
}

// Tuple converts the key to the shared read-path identity shape.
func (k PodRestartKey) Tuple() model.PodTuple {
	return model.PodTuple{
		Namespace: k.Namespace, Service: k.Service,
		Pod: k.PodName, RestartTimeMs: k.RestartTimeMs,
	}
}

func (k PodRestartKey) dir(dataDir string) string {
	return filepath.Join(dataDir, "pods", k.Namespace, k.Service, k.PodName,
		strconv.FormatInt(k.RestartTimeMs, 10))
}

// Open mounts the data dir, takes the exclusive collector.lock (03-lifecycle.md
// §3.1), and opens the SQLite metadata. It does NOT run recovery; call
// Recover before serving traffic.
func Open(cfg Config) (*Store, error) {
	cfg = cfg.Normalize()
	if err := os.MkdirAll(filepath.Join(cfg.DataDir, "pods"), 0o755); err != nil {
		return nil, errors.Wrap(err, "create data dir")
	}
	lock, err := acquireLock(filepath.Join(cfg.DataDir, "collector.lock"))
	if err != nil {
		return nil, err
	}
	db, err := openMetaDb(cfg)
	if err != nil {
		_ = lock.Close()
		return nil, err
	}
	return &Store{cfg: cfg, db: db, lock: lock, pods: map[string]*PodRestart{}}, nil
}

// acquireLock flocks collector.lock so two collector processes cannot share a
// PV (01-write-contract.md §8). It fails fast on contention: the startup wait
// loop is the caller's lifecycle policy.
func acquireLock(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, errors.Wrap(err, "open collector.lock")
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, errors.Wrap(err, "collector.lock held by another process")
	}
	return f, nil
}

// Close releases SQLite handles and the PV lock. Pod-restarts still open lose
// their WAL footers, exactly as a crash would; recovery replays them.
func (s *Store) Close() error {
	err := s.db.Close()
	if s.lock != nil {
		_ = syscall.Flock(int(s.lock.Fd()), syscall.LOCK_UN)
		_ = s.lock.Close()
		s.lock = nil
	}
	return err
}

func (s *Store) Config() Config { return s.cfg }

// OpenPodRestart creates the on-PV layout and WALs for a new agent connection.
func (s *Store) OpenPodRestart(key PodRestartKey) (*PodRestart, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if pr, ok := s.pods[key.String()]; ok && !pr.closed {
		return pr, nil
	}

	dir := key.dir(s.cfg.DataDir)
	for _, sub := range []string{StreamTrace, StreamSql, StreamXml} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0o755); err != nil {
			return nil, errors.Wrap(err, "create pod-restart dir")
		}
	}
	pr := &PodRestart{
		Key:      key,
		store:    s,
		dir:      dir,
		dict:     map[int]string{},
		dictIds:  map[string]int{},
		chunks:   map[uint64][]ChunkRef{},
		segments: map[*Segment]struct{}{},
	}
	var err error
	openWal := func(name string) *Wal {
		if err != nil {
			return nil
		}
		var w *Wal
		w, err = OpenWal(filepath.Join(dir, name), s.cfg.DictFsyncRecords, s.cfg.DictFsyncInterval)
		return w
	}
	pr.dictWal = openWal("dictionary.wal")
	pr.paramsWal = openWal("params.wal")
	pr.suspendWal = openWal("suspend.wal")
	pr.callsWal = openWal("calls.wal")
	if err != nil {
		return nil, err
	}
	if err := s.db.UpsertPodRestart(key, time.Now().UnixMilli()); err != nil {
		return nil, err
	}
	s.pods[key.String()] = pr
	return pr, nil
}

// PodRestart returns the state of a known pod-restart, live or recovered.
func (s *Store) PodRestart(key PodRestartKey) (*PodRestart, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pr, ok := s.pods[key.String()]
	return pr, ok
}

// PodRestartKeys lists the pod-restarts the store currently tracks.
func (s *Store) PodRestartKeys() []PodRestartKey {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]PodRestartKey, 0, len(s.pods))
	for _, pr := range s.pods {
		keys = append(keys, pr.Key)
	}
	return keys
}

// Segments exposes the segment catalog for tests and the future seal pass.
func (s *Store) Segments(key PodRestartKey) ([]SegmentRow, error) {
	return s.db.Segments(key.String())
}

// Buckets lists the call-index partitions.
func (s *Store) Buckets() ([]int64, error) { return s.db.Buckets() }

// Calls reads one bucket's call-index rows.
func (s *Store) Calls(bucket int64) ([]CallIndexRow, error) { return s.db.Calls(bucket) }

// LocalParquet lists a pod-restart's sealed parquet files still held locally;
// UploadedAtMs stays nil until the Uploader confirms the S3 PUT.
func (s *Store) LocalParquet(key PodRestartKey) ([]ParquetLocalFile, error) {
	return s.db.LocalParquet(key.String())
}

// OpenSegment starts the hot-store segment for one agent stream file. seq is
// the agent's file index (serverRollingSequenceId + 1); see SegmentFileName.
func (pr *PodRestart) OpenSegment(stream string, seq int) (*Segment, error) {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	if pr.closed {
		return nil, errors.Errorf("pod-restart %s is closed", pr.Key)
	}
	path := filepath.Join(pr.dir, stream, SegmentFileName(seq))
	w, err := OpenSegment(path, stream, seq)
	if err != nil {
		return nil, err
	}
	if err := pr.store.db.UpsertSegment(pr.Key.String(), stream, seq, path, time.Now().UnixMilli()); err != nil {
		_ = w.Close()
		return nil, err
	}
	seg := &Segment{w: w}
	pr.segments[seg] = struct{}{}
	return seg, nil
}

// Write appends raw stream bytes to the segment.
func (seg *Segment) Write(p []byte) (int, error) { return seg.w.Write(p) }

// LogicalSize reports the segment's uncompressed size so far.
func (seg *Segment) LogicalSize() int64 { return seg.w.LogicalSize() }

// Seq reports the agent stream-file index the segment is named by.
func (seg *Segment) Seq() int { return seg.w.Seq }

// AddChunk indexes one parsed logical trace chunk: into chunk_index[threadId]
// and into the segment's time range for the catalog (01 §4.3).
func (pr *PodRestart) AddChunk(seg *Segment, threadId uint64, offset int64, length int, startMs int64) {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	pr.chunks[threadId] = append(pr.chunks[threadId], ChunkRef{
		RollingSeq: seg.w.Seq, Offset: offset, Length: length, StartMs: startMs,
	})
	if seg.timeMinMs == nil || startMs < *seg.timeMinMs {
		v := startMs
		seg.timeMinMs = &v
	}
	if seg.timeMaxMs == nil || startMs > *seg.timeMaxMs {
		v := startMs
		seg.timeMaxMs = &v
	}
}

// FinalizeSegment closes the gzip stream and completes the catalog row.
func (pr *PodRestart) FinalizeSegment(seg *Segment) error {
	pr.mu.Lock()
	delete(pr.segments, seg)
	pr.mu.Unlock()
	if err := seg.w.Close(); err != nil {
		return err
	}
	return pr.store.db.FinalizeSegment(pr.Key.String(), seg.w.Stream, seg.w.Seq,
		seg.w.LogicalSize(), seg.timeMinMs, seg.timeMaxMs)
}

// SetTimerStart records the trace stream's 8-byte epoch. Every trace file of a
// pod-restart carries the same value (Dumper writes TimerCache.startTime on
// each rotation), so the first one wins.
func (pr *PodRestart) SetTimerStart(ms int64) {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	if pr.timerStartMs == 0 {
		pr.timerStartMs = ms
	}
}

// TimerStartMs reports the trace epoch, or 0 before the first trace file.
func (pr *PodRestart) TimerStartMs() int64 {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	return pr.timerStartMs
}

// Closed reports whether the pod-restart stopped accepting writes.
func (pr *PodRestart) Closed() bool {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	return pr.closed
}

// Finalized reports whether Close finished: WAL footers written and the
// catalog row closed.
func (pr *PodRestart) Finalized() bool {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	return pr.finalized
}

// AppendDictionaryWord persists one dictionary word and returns its id (the
// arrival index within the pod-restart). The WAL body follows 01 §3.2:
// varint(word_id) varint(word_len) word_bytes.
func (pr *PodRestart) AppendDictionaryWord(word string) (int, error) {
	pr.mu.Lock()
	id := pr.nextWordId
	pr.nextWordId++
	pr.dict[id] = word
	pr.dictIds[word] = id
	pr.mu.Unlock()

	body := make([]byte, 0, 2*binary.MaxVarintLen64+len(word))
	body = binary.AppendUvarint(body, uint64(id))
	body = binary.AppendUvarint(body, uint64(len(word)))
	body = append(body, word...)
	_, err := pr.dictWal.Append(body)
	return id, err
}

// ResetDictionary handles a dictionary stream opened with resetRequired: the
// agent re-sends the whole dictionary from index 0 (01 §3.7).
func (pr *PodRestart) ResetDictionary() {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	pr.nextWordId = 0
	pr.dict = map[int]string{}
	pr.dictIds = map[string]int{}
}

// Dictionary returns a copy of the in-RAM dictionary.
func (pr *PodRestart) Dictionary() map[int]string {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	out := make(map[int]string, len(pr.dict))
	for k, v := range pr.dict {
		out[k] = v
	}
	return out
}

// DictId resolves a word to its dictionary id, the reverse lookup §5.6 needs
// for the call.red marker.
func (pr *PodRestart) DictId(word string) (int, bool) {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	id, ok := pr.dictIds[word]
	return id, ok
}

// chunkSnapshot copies the whole chunk index for a seal walk.
func (pr *PodRestart) chunkSnapshot() map[uint64][]ChunkRef {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	out := make(map[uint64][]ChunkRef, len(pr.chunks))
	for threadId, refs := range pr.chunks {
		out[threadId] = append([]ChunkRef(nil), refs...)
	}
	return out
}

// FlushSegments pushes every open segment's gzip state to disk so a seal pass
// on a live pod-restart reads all indexed chunks.
func (pr *PodRestart) FlushSegments() error {
	pr.mu.Lock()
	segments := make([]*Segment, 0, len(pr.segments))
	for seg := range pr.segments {
		segments = append(segments, seg)
	}
	pr.mu.Unlock()
	for _, seg := range segments {
		if err := seg.w.Flush(); err != nil {
			return err
		}
	}
	return nil
}

// ChunkIndex returns a copy of chunk_index[threadId].
func (pr *PodRestart) ChunkIndex(threadId uint64) []ChunkRef {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	return append([]ChunkRef(nil), pr.chunks[threadId]...)
}

// Threads lists the thread ids present in the chunk index.
func (pr *PodRestart) Threads() []uint64 {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	out := make([]uint64, 0, len(pr.chunks))
	for id := range pr.chunks {
		out = append(out, id)
	}
	return out
}

// AppendParam persists one params-stream record to params.wal.
func (pr *PodRestart) AppendParam(p ParamRecord) error {
	body, err := json.Marshal(p)
	if err != nil {
		return errors.Wrap(err, "encode params.wal record")
	}
	_, err = pr.paramsWal.Append(body)
	return err
}

// AppendSuspend persists one stop-the-world pause to suspend.wal and mirrors
// it in RAM for the index-time suspend_ms attribution.
func (pr *PodRestart) AppendSuspend(timeMs int64, durationMs int) error {
	rec := SuspendPause{TimeMs: timeMs, DurationMs: durationMs}
	body, err := json.Marshal(rec)
	if err != nil {
		return errors.Wrap(err, "encode suspend.wal record")
	}
	if _, err := pr.suspendWal.Append(body); err != nil {
		return err
	}
	pr.mu.Lock()
	pr.pauses = append(pr.pauses, rec)
	pr.mu.Unlock()
	return nil
}

// SuspendPauses snapshots the pod-restart's global suspension timeline as
// seen so far: the R7 tree path intersects node work intervals with it, and
// the call index attributes the provisional per-call suspend_ms from it.
func (pr *PodRestart) SuspendPauses() []SuspendPause {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	return append([]SuspendPause(nil), pr.pauses...)
}

// AppendCall persists one decoded Call record (01 §4.3 step 1-2): the full
// record to calls.wal, then the index row into its bucket's partition.
func (pr *PodRestart) AppendCall(tsMs int64, call data.Call) error {
	body, err := json.Marshal(CallWalRecord{TsMs: tsMs, Call: call})
	if err != nil {
		return errors.Wrap(err, "encode calls.wal record")
	}
	offset, err := pr.callsWal.Append(body)
	if err != nil {
		return err
	}
	return pr.indexCall(tsMs, call, offset)
}

// indexCall inserts the SQLite call-index row for a Call already in calls.wal.
// suspend_ms is the provisional index-time intersection with the pauses seen
// so far (the seal re-derives it, 01 §5.1 step 4); the wire carries no
// per-call suspend field.
func (pr *PodRestart) indexCall(tsMs int64, call data.Call, walOffset int64) error {
	cfg := pr.store.cfg
	errorFlag := pr.hasErrorMarker(call)
	pr.mu.Lock()
	suspendMs := suspendOverlapMs(pr.pauses, tsMs, int(call.Duration))
	pr.mu.Unlock()
	row := CallIndexRow{
		PodRestart:     pr.Key.String(),
		TraceFileIndex: call.TraceFileIndex,
		BufferOffset:   call.BufferOffset,
		RecordIndex:    call.RecordIndex,
		TsMs:           tsMs,
		DurationMs:     int(call.Duration),
		MethodId:       call.Method,
		ThreadName:     call.ThreadName,
		RetentionClass: cfg.RetentionClass(time.Duration(call.Duration)*time.Millisecond, errorFlag),
		ErrorFlag:      errorFlag,
		CpuTimeMs:      int64(call.CpuTime),
		WaitTimeMs:     int64(call.WaitTime),
		MemoryUsed:     int64(call.MemoryUsed),
		QueueWaitMs:    int(call.QueueWaitDuration),
		SuspendMs:      suspendMs,
		ChildCalls:     int(call.Calls),
		Transactions:   int(call.Transactions),
		LogsGenerated:  int64(call.LogsGenerated),
		LogsWritten:    int64(call.LogsWritten),
		FileRead:       int64(call.FileRead),
		FileWritten:    int64(call.FileWritten),
		NetRead:        int64(call.NetRead),
		NetWritten:     int64(call.NetWritten),
		ParamsJson:     pr.paramsJson(call.Params),
		CallsWalOffset: walOffset,
	}
	return pr.store.db.InsertCall(cfg.Bucket(tsMs), row)
}

// hasErrorMarker implements 01 §5.6: error_flag := dictId("call.red") ∈
// keys(Call.Params).
func (pr *PodRestart) hasErrorMarker(call data.Call) bool {
	pr.mu.Lock()
	id, known := pr.dictIds[errorMarkerParam]
	pr.mu.Unlock()
	if !known {
		return false
	}
	_, has := call.Params[id]
	return has
}

// paramsJson resolves param tag ids against the dictionary (01 §5.1 step 1)
// and renders {"name": [values]}; an id with no dictionary entry keeps a
// "#<id>" placeholder rather than dropping the values.
func (pr *PodRestart) paramsJson(params map[data.TagId][]string) string {
	if len(params) == 0 {
		return ""
	}
	pr.mu.Lock()
	resolved := make(map[string][]string, len(params))
	for id, values := range params {
		name, ok := pr.dict[id]
		if !ok {
			name = fmt.Sprintf("#%d", id)
		}
		resolved[name] = values
	}
	pr.mu.Unlock()
	out, err := json.Marshal(resolved)
	if err != nil {
		return ""
	}
	return string(out)
}

// Close finalizes the pod-restart: leftover segments are closed, the WALs get
// their CRC footers, and the catalog row is marked closed (03 §5.2-5.3 without
// the seal/upload steps, which land with the seal pass).
func (pr *PodRestart) Close() error {
	pr.mu.Lock()
	if pr.closed {
		pr.mu.Unlock()
		return nil
	}
	pr.closed = true
	leftover := make([]*Segment, 0, len(pr.segments))
	for seg := range pr.segments {
		leftover = append(leftover, seg)
	}
	pr.segments = map[*Segment]struct{}{}
	pr.mu.Unlock()

	var firstErr error
	keep := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	for _, seg := range leftover {
		keep(seg.w.Close())
		keep(pr.store.db.FinalizeSegment(pr.Key.String(), seg.w.Stream, seg.w.Seq,
			seg.w.LogicalSize(), seg.timeMinMs, seg.timeMaxMs))
	}
	for _, w := range []*Wal{pr.dictWal, pr.paramsWal, pr.suspendWal, pr.callsWal} {
		if w != nil {
			keep(w.Close())
		}
	}
	keep(pr.store.db.ClosePodRestart(pr.Key, time.Now().UnixMilli()))
	pr.mu.Lock()
	pr.finalized = true
	pr.mu.Unlock()
	return firstErr
}
