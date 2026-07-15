package hotstore

// Read-side surface of the hot store for the collector's /internal/v1 API
// (02-read-contract.md §3): the SQLite call index answers /calls, the seal
// pass's chunk-walk machinery assembles /calls/{pk}/trace, and the call
// partitions bound the /health/hot-window report. Everything here reads the
// replica's own PV; S3 is the query service's job.

import (
	"context"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
)

// ErrBlobUnavailable marks a trace blob the hot store cannot assemble: the
// pointer resolves to no indexed chunk (idle-timeout release, bad pointer) or
// the chunk chain lost bytes (segment evicted, torn tail). The internal API
// maps it to 404, mirroring the §2.4 "blob is either present or absent".
var ErrBlobUnavailable = errors.New("trace blob unavailable")

// HotWindowOldestMs reports the earliest ts_ms this replica still serves —
// the /internal/v1/health/hot-window value the query service derives the cold
// cutoff from (02 §3, §4.3). ok is false when the index holds no calls.
func (s *Store) HotWindowOldestMs() (int64, bool, error) {
	buckets, err := s.db.Buckets()
	if err != nil {
		return 0, false, err
	}
	for _, bucket := range buckets { // ascending; the first populated one wins
		min, err := s.db.MinTsMs(bucket)
		if err != nil {
			return 0, false, err
		}
		if min != nil {
			return *min, true, nil
		}
	}
	return 0, false, nil
}

// CallsInWindow reads one partition's index rows with ts_ms in [fromMs, toMs).
// Rows come back unordered: the caller applies the (ts_ms DESC, pk ASC) order
// with the shared component-wise comparator — the scalar pod_restart string
// does NOT collate like the PK components (02 §2.3.1).
func (s *Store) CallsInWindow(bucket, fromMs, toMs int64) ([]CallIndexRow, error) {
	return s.db.CallsInWindow(bucket, fromMs, toMs)
}

// FindCall looks one call up by its PK across the partitions (/internal/v1
// /calls/{pk}, 02 §3). A bare PK carries no time, so every partition is
// probed; the point SELECT rides the partition's primary key.
func (s *Store) FindCall(key PodRestartKey, traceFileIndex, bufferOffset, recordIndex int) (CallIndexRow, bool, error) {
	buckets, err := s.db.Buckets()
	if err != nil {
		return CallIndexRow{}, false, err
	}
	for i := len(buckets) - 1; i >= 0; i-- { // recent data first
		row, ok, err := s.db.FindCall(buckets[i], key.String(), traceFileIndex, bufferOffset, recordIndex)
		if err != nil || ok {
			return row, ok, err
		}
	}
	return CallIndexRow{}, false, nil
}

// PodWindows reports, per pod-restart, the [min, max] ts_ms bounds of its
// indexed calls across all partitions — the hot side of the /pods union
// (02 §2.7).
func (s *Store) PodWindows() (map[string][2]int64, error) {
	buckets, err := s.db.Buckets()
	if err != nil {
		return nil, err
	}
	out := map[string][2]int64{}
	for _, bucket := range buckets {
		windows, err := s.db.PodWindows(bucket)
		if err != nil {
			return nil, err
		}
		for podRestart, w := range windows {
			cur, ok := out[podRestart]
			if !ok {
				out[podRestart] = w
				continue
			}
			if w[0] < cur[0] {
				cur[0] = w[0]
			}
			if w[1] > cur[1] {
				cur[1] = w[1]
			}
			out[podRestart] = cur
		}
	}
	return out, nil
}

// DictionaryWords renders the dictionary as the dense word array of the §2.6
// snapshot: words[id] = word. The wire dictionary is one id space, so the
// snapshot's methods and params arrays both carry this full list (01 §3.6);
// the S3 snapshot upload and the live /internal dictionary share this shape.
func (pr *PodRestart) DictionaryWords() []string {
	dict := pr.Dictionary()
	maxId := -1
	for id := range dict {
		if id > maxId {
			maxId = id
		}
	}
	words := make([]string, maxId+1)
	for id, word := range dict {
		words[id] = word
	}
	return words
}

// AssembleTraceBlob builds one call's blob for /internal/v1/calls/{pk}/trace
// with the machinery the seal pass uses (01 §4.3/§4.5): the thread's chunk
// chain from the Call pointer, whole chunks appended behind the 8-byte epoch
// prefix, and the same depth walk (consumeChunk) stopping at the depth-0
// exit. Failures that mean "this replica does not hold the blob" wrap
// ErrBlobUnavailable.
func (s *Store) AssembleTraceBlob(ctx context.Context, key PodRestartKey, traceFileIndex int, bufferOffset int64, recordIndex int) ([]byte, error) {
	pr, ok := s.PodRestart(key)
	if !ok {
		return nil, errors.Wrapf(ErrBlobUnavailable, "unknown pod-restart %s", key)
	}
	// A live pod-restart still holds unflushed bytes in its gzip writers, as
	// in Seal: push them out so the walk below sees every indexed chunk.
	if !pr.Closed() {
		if err := pr.FlushSegments(); err != nil {
			return nil, err
		}
	}

	refs, threadId, found := pr.chunkChainFrom(traceFileIndex, bufferOffset)
	if !found {
		return nil, errors.Wrapf(ErrBlobUnavailable,
			"pointer (%d, %d) of %s resolves to no indexed chunk", traceFileIndex, bufferOffset, key)
	}

	scratchDir := filepath.Join(pr.dir, "parquet-sealing")
	if err := os.MkdirAll(scratchDir, 0o755); err != nil {
		return nil, errors.Wrap(err, "create blob scratch dir")
	}
	var prefix [8]byte
	binary.BigEndian.PutUint64(prefix[:], uint64(pr.TimerStartMs()))
	row := &sealRow{
		idx: CallIndexRow{PodRestart: key.String(), TraceFileIndex: traceFileIndex,
			BufferOffset: int(bufferOffset), RecordIndex: recordIndex},
		srcSegs: map[segKey]struct{}{},
	}
	row.blob = newBlobBuffer(scratchDir, s.cfg.SealSpillBytes, prefix[:])
	defer row.freeBlob()
	a := &assembly{row: row, threadId: threadId}

	var reader *segmentReader
	seq, pos := -1, int64(0)
	defer func() {
		if reader != nil {
			_ = reader.Close()
		}
	}()
	for _, ref := range refs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if reader == nil || ref.RollingSeq != seq || ref.Offset < pos {
			if reader != nil {
				_ = reader.Close()
				reader = nil
			}
			r, err := openSegmentReader(filepath.Join(pr.dir, StreamTrace, SegmentFileName(ref.RollingSeq)))
			if err != nil {
				return nil, errors.Wrapf(ErrBlobUnavailable, "segment %d of %s is unreadable: %v",
					ref.RollingSeq, key, err)
			}
			reader, seq, pos = r, ref.RollingSeq, 0
		}
		if _, err := io.CopyN(io.Discard, reader, ref.Offset-pos); err != nil {
			return nil, errors.Wrapf(ErrBlobUnavailable, "segment %d of %s lost its tail: %v", seq, key, err)
		}
		data := make([]byte, ref.Length)
		if _, err := io.ReadFull(reader, data); err != nil {
			return nil, errors.Wrapf(ErrBlobUnavailable, "segment %d of %s lost its tail: %v", seq, key, err)
		}
		pos = ref.Offset + int64(ref.Length)
		s.consumeChunk(ctx, pr, a, seq, data)
		if a.done {
			break
		}
	}
	if !a.done || row.truncated != "" {
		return nil, errors.Wrapf(ErrBlobUnavailable,
			"chunk chain of (%d, %d, %d) of %s ends before the depth-0 exit",
			traceFileIndex, bufferOffset, recordIndex, key)
	}
	return row.blob.Bytes()
}

// chunkChainFrom finds the thread whose chunk chain starts a call at the
// pointer and returns the chain's tail from that chunk on.
func (pr *PodRestart) chunkChainFrom(traceFileIndex int, bufferOffset int64) ([]ChunkRef, uint64, bool) {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	for threadId, chain := range pr.chunks {
		for i, ref := range chain {
			if ref.RollingSeq == traceFileIndex && ref.Offset == bufferOffset {
				return append([]ChunkRef(nil), chain[i:]...), threadId, true
			}
		}
	}
	return nil, 0, false
}
