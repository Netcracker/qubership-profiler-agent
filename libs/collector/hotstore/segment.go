package hotstore

import (
	"compress/gzip"
	"fmt"
	"os"
	"sync"

	"github.com/pkg/errors"
)

// Streams that live in the hot-store segment catalog. dictionary, params, and
// suspend use WALs instead (03-lifecycle.md §3.2).
const (
	StreamTrace = "trace"
	StreamSql   = "sql"
	StreamXml   = "xml"
)

// segmentFlushBytes bounds how much logical (uncompressed) data may sit in the
// gzip writer before it is flushed to the file. Recovery tolerates a torn gzip
// tail, so the flush cadence only bounds how many trailing bytes a crash can
// lose; it is not a durability guarantee (only WALs fsync, 01 §3.3).
const segmentFlushBytes = 256 * 1024

// SegmentFileName renders the on-PV name of a hot-store segment. seq is the
// AGENT's stream-file index: serverRollingSequenceId + 1, the value the agent
// writes into every Call's trace_file_index. Naming a segment by the echoed id
// instead is an off-by-one that silently corrupts all trace addressing
// (01-write-contract.md §4.4).
func SegmentFileName(seq int) string {
	return fmt.Sprintf("%06d.gz", seq)
}

// SegmentWriter appends one demultiplexed agent stream file to a gzip segment
// on the PV, tracking the logical (uncompressed) offset that Call pointers and
// trace tags address (01-write-contract.md §4.4). The mutex serializes the
// ingest goroutine's Write against a seal pass flushing a live segment.
type SegmentWriter struct {
	Stream string
	Seq    int // agent stream-file index; also the file name
	Path   string

	mu          sync.Mutex
	f           *os.File
	gz          *gzip.Writer
	logicalSize int64
	sinceFlush  int64
}

// OpenSegment creates the segment file. It fails if the segment already
// exists: rolling sequence ids never repeat within a pod-restart.
func OpenSegment(path, stream string, seq int) (*SegmentWriter, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, errors.Wrap(err, "open segment")
	}
	return &SegmentWriter{Stream: stream, Seq: seq, Path: path, f: f, gz: gzip.NewWriter(f)}, nil
}

// Write appends raw stream bytes and advances the logical offset.
func (s *SegmentWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, err := s.gz.Write(p)
	s.logicalSize += int64(n)
	s.sinceFlush += int64(n)
	if err == nil && s.sinceFlush >= segmentFlushBytes {
		err = s.gz.Flush()
		s.sinceFlush = 0
	}
	return n, err
}

// Flush makes everything written so far readable from the file; a seal pass
// calls it before walking a live pod-restart's segments.
func (s *SegmentWriter) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.gz == nil || s.sinceFlush == 0 {
		return nil
	}
	s.sinceFlush = 0
	return errors.Wrap(s.gz.Flush(), "flush segment gzip")
}

// LogicalSize reports the uncompressed bytes written so far — the offset the
// next byte of the stream lands at.
func (s *SegmentWriter) LogicalSize() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.logicalSize
}

// Close finalizes the gzip stream and the file.
func (s *SegmentWriter) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.gz == nil {
		return nil
	}
	gzErr := s.gz.Close()
	fErr := s.f.Close()
	s.gz = nil
	if gzErr != nil {
		return errors.Wrap(gzErr, "close segment gzip")
	}
	return errors.Wrap(fErr, "close segment file")
}
