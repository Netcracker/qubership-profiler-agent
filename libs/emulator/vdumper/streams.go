package vdumper

import (
	"bytes"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/common"
	"github.com/Netcracker/qubership-profiler-backend/libs/emulator"
	"github.com/Netcracker/qubership-profiler-backend/libs/emulator/wire"
)

// Local size-rotation thresholds, mirroring the Dumper.initStreams defaults;
// 0 disables the local size check (phrase streams rotate only on the
// collector's rotation period).
const (
	traceRotateSize = 100 * 1024 * 1024
	callsRotateSize = 10 * 1024 * 1024
	valueRotateSize = 100 * 1024 * 1024 // sql and xml
)

// streamState is one rolling remote stream: the Go mirror of
// CompressedLocalAndRemoteOutputStream + RemoteAndLocalOutputStream for the
// remote-only case. All methods run on the dumper goroutine.
type streamState struct {
	name            string
	phrase          bool // dictionary, suspend, params are phrase-framed
	localRotateSize int
	oneShot         bool // params: content is written at open, then the stream idles

	// Per-connection wire state.
	tr        Transport
	handle    common.Uuid
	fileIndex int // 1-based after open, like the agent's rolling index
	// Collector-provided rotation policy (06 §4).
	rotationPeriodMs     uint64
	requiredRotationSize uint64
	lastRotated          time.Time
	closed               bool // one-shot stream finished its payload

	// fileOffset counts bytes written into the current file — the agent's
	// stream.size(), which the calls records reference as buffer_offset.
	fileOffset int

	buf       bytes.Buffer // the 1 KB BufferedOutputStream mirror
	phraseBuf *wire.PhraseBuffer

	stats StatsListener
}

func newStreamState(name string, phrase bool, localRotateSize int, oneShot bool, stats StatsListener) *streamState {
	s := &streamState{name: name, phrase: phrase, localRotateSize: localRotateSize, oneShot: oneShot, stats: stats}
	if phrase {
		s.phraseBuf = wire.NewPhraseBuffer(wire.MaxPhraseSize, s.writeRaw)
	}
	return s
}

// open sends INIT_STREAM_V2 for this stream and adopts the collector's rolling
// sequence and rotation policy. resetRequired follows the caller (true only
// for the dictionary while nothing has been sent on this connection).
func (s *streamState) open(t Transport, now time.Time, resetRequired bool) (emulator.InitStreamReply, error) {
	reply, err := t.InitStream(s.name, s.fileIndex, resetRequired)
	if err != nil {
		return reply, err
	}
	s.tr = t
	s.handle = reply.Handle
	s.fileIndex = reply.SeqId + 1
	s.rotationPeriodMs = reply.RotationPeriodMs
	s.requiredRotationSize = reply.RequiredRotationSize
	s.lastRotated = now
	s.fileOffset = 0
	s.closed = false
	s.stats.StreamOpened(s.name, s.fileIndex, resetRequired)
	return reply, nil
}

// resetForConnection drops the per-connection state before a reconnect. The
// rolling sequence restarts at zero: the agent re-initializes each stream's
// index on every Dumper.initialize, and the collector scopes sequences to the
// pod-restart anyway.
func (s *streamState) resetForConnection() {
	s.tr = nil
	s.handle = common.Uuid{}
	s.fileIndex = 0
	s.fileOffset = 0
	s.closed = false
	s.buf.Reset()
	if s.phrase {
		s.phraseBuf = wire.NewPhraseBuffer(wire.MaxPhraseSize, s.writeRaw)
	}
}

// write appends raw stream bytes (non-phrase streams and file headers).
func (s *streamState) write(p []byte) error {
	s.fileOffset += len(p)
	return s.writeRaw(p)
}

// writePhrase appends one committed phrase to a phrase-framed stream: the
// framing counts toward the file offset the same way PhraseOutputStream's
// frames reach the wire.
func (s *streamState) writePhrase(p []byte) error {
	s.fileOffset += len(p)
	if err := s.phraseBuf.Write(p); err != nil {
		return err
	}
	return s.phraseBuf.ClosePhrase()
}

// writeRaw is the 1 KB chop: full MaxBufSize payloads leave as RCV_DATA as the
// buffer fills, the tail waits for flushTail (BufferedOutputStream(1024) over
// RollingChunkStream).
func (s *streamState) writeRaw(p []byte) error {
	s.buf.Write(p)
	for s.buf.Len() >= emulator.MaxBufSize {
		if err := s.emit(s.buf.Next(emulator.MaxBufSize)); err != nil {
			return err
		}
	}
	return nil
}

// flushTail pushes everything buffered (committed phrases, then the partial
// RCV_DATA payload) without the ack cycle; the caller follows with
// Transport.Flush.
func (s *streamState) flushTail() error {
	if s.phrase {
		if err := s.phraseBuf.Flush(); err != nil {
			return err
		}
	}
	if s.buf.Len() > 0 {
		if err := s.emit(s.buf.Next(s.buf.Len())); err != nil {
			return err
		}
	}
	return nil
}

func (s *streamState) emit(chunk []byte) error {
	if err := s.tr.CommandRcvData(s.name, s.handle, chunk); err != nil {
		return err
	}
	s.stats.BytesSent(s.name, len(chunk))
	return nil
}

// needsRotation applies CompressedLocalAndRemoteOutputStream.rotateIfRequired:
// the collector's rotation period, or the size threshold
// min(localRotateSize, requiredRotationSize), whichever exists.
func (s *streamState) needsRotation(now time.Time) bool {
	if s.rotationPeriodMs > 0 &&
		now.Sub(s.lastRotated) > time.Duration(s.rotationPeriodMs)*time.Millisecond {
		return true
	}
	threshold := 0
	switch {
	case s.localRotateSize > 0 && s.requiredRotationSize > 0:
		threshold = min(s.localRotateSize, int(s.requiredRotationSize))
	case s.localRotateSize > 0:
		threshold = s.localRotateSize
	case s.requiredRotationSize > 0:
		threshold = int(s.requiredRotationSize)
	}
	return threshold > 0 && !s.closed && s.fileOffset > threshold
}
