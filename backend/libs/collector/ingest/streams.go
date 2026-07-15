package ingest

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"

	"github.com/Netcracker/qubership-profiler-backend/libs/collector/hotstore"
	"github.com/Netcracker/qubership-profiler-backend/libs/common"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/Netcracker/qubership-profiler-backend/libs/parser/pipe"
	model "github.com/Netcracker/qubership-profiler-backend/libs/protocol"
	"github.com/pkg/errors"
)

// noPhraseLimit disables DictionaryPipeReader's phrase cap: a live stream is
// bounded by the connection, not by a fixture size.
const noPhraseLimit = 1 << 30

// fileIngest is the pipeline of one agent stream file: an optional gzip
// segment (trace/sql/xml) plus an optional decoder goroutine fed through an
// io.Pipe (everything except sql/xml, which are stored without parsing).
type fileIngest struct {
	stream string
	// handle is the RCV_DATA handle this file is keyed by in podIngest.byHandle;
	// stored so a rotation or a stream close can drop the dead entry instead of
	// letting it accumulate until disconnect (wire-LOW).
	handle common.Uuid
	seg    *hotstore.Segment
	pw     *io.PipeWriter
	done   chan struct{}
}

// write appends one RCV_DATA payload: segment first, then the decoder. The
// pipe applies backpressure — the connection goroutine blocks until the
// decoder catches up — so ingest never buffers unbounded bytes.
func (fi *fileIngest) write(p []byte) error {
	if fi.seg != nil {
		if _, err := fi.seg.Write(p); err != nil {
			return errors.Wrapf(err, "append %s segment", fi.stream)
		}
	}
	if fi.pw != nil {
		if _, err := fi.pw.Write(p); err != nil {
			return errors.Wrapf(err, "feed %s decoder", fi.stream)
		}
	}
	return nil
}

// finalize ends the file: the decoder drains to EOF and the segment's catalog
// row completes. Called on rotation and on disconnect; pi.mu is held.
func (pi *podIngest) finalize(fi *fileIngest) error {
	if fi.pw != nil {
		_ = fi.pw.Close()
		<-fi.done
	}
	if fi.seg != nil {
		return pi.pr.FinalizeSegment(fi.seg)
	}
	return nil
}

// openFile builds the pipeline for one just-registered stream file.
// agentFileIndex names hot-store segments (01 §4.4); WAL-backed streams ignore
// it, since their WALs span the whole pod-restart.
func (pi *podIngest) openFile(ctx context.Context, streamType string, agentFileIndex int) (*fileIngest, error) {
	pr := pi.pr
	switch streamType {
	case model.StreamTrace:
		seg, err := pr.OpenSegment(hotstore.StreamTrace, agentFileIndex)
		if err != nil {
			return nil, err
		}
		return pi.startDecoder(ctx, streamType, seg, func(src io.Reader) error {
			decodeTrace(ctx, pr, seg, src)
			return nil
		}), nil

	case model.StreamSql, model.StreamXml:
		seg, err := pr.OpenSegment(streamType, agentFileIndex)
		if err != nil {
			return nil, err
		}
		// Offset-addressed value bytes: stored raw, never parsed on ingest.
		return &fileIngest{stream: streamType, seg: seg}, nil

	case model.StreamCalls:
		return pi.startDecoder(ctx, streamType, nil, func(src io.Reader) error {
			for item := range pipe.CallsPipeReader(ctx, pipe.NewPipeReader(src, false)) {
				if err := pr.AppendCall(item.Time.UnixMilli(), item.Call); err != nil {
					// №2: swallowing a write failure here (ENOSPC on calls.wal
					// being the canonical case) silently loses calls. Failing the
					// stream tears the connection down through the pipe, so the
					// agent re-sends after reconnect instead of losing the tail.
					return errors.Wrapf(err, "index call of %v", pr.Key)
				}
			}
			return nil
		}), nil

	case model.StreamDictionary:
		return pi.startDecoder(ctx, streamType, nil, func(src io.Reader) error {
			// A malformed dictionary is fatal: every trace tag references it by
			// id, so a shifted or truncated dictionary corrupts every later name.
			// The reader reports whether it stopped clean or on a bad byte, and a
			// non-clean stop fails the stream so the agent resends the whole
			// dictionary with resetRequired (06 §6, №21).
			rd := pipe.NewPipeReader(src, false)
			for item := range pipe.DictionaryPipeReader(ctx, rd, noPhraseLimit) {
				if _, err := pr.AppendDictionaryWord(item.Value); err != nil {
					log.Error(ctx, err, "append dictionary word of %v", pr.Key)
				}
			}
			return rd.Err()
		}), nil

	case model.StreamParams:
		return pi.startDecoder(ctx, streamType, nil, func(src io.Reader) error {
			for item := range pipe.ParamsPipeReader(ctx, pipe.NewPipeReader(src, false)) {
				err := pr.AppendParam(hotstore.ParamRecord{
					Name: item.Name, IsIndex: item.IsIndex, IsList: item.IsList,
					Order: item.Order, Signature: item.Signature,
				})
				if err != nil {
					log.Error(ctx, err, "append param of %v", pr.Key)
				}
			}
			return nil
		}), nil

	case model.StreamSuspend:
		return pi.startDecoder(ctx, streamType, nil, func(src io.Reader) error {
			for item := range pipe.SuspendPipeReader(ctx, pipe.NewPipeReader(src, false)) {
				if err := pr.AppendSuspend(item.Time.UnixMilli(), item.Amount); err != nil {
					log.Error(ctx, err, "append suspend of %v", pr.Key)
				}
			}
			return nil
		}), nil

	case model.StreamGc:
		// Pre-v3.1.4 agents open this stream unconditionally when streaming
		// remotely (Dumper.java's gcOs), regardless of GC-log harvesting
		// being enabled. GCDumper was removed in v3.1.4 (commit ac804ee3);
		// GC-log collection now lives in diagtools, so there is nowhere to
		// route these bytes in the new architecture. Register the stream so
		// INIT_STREAM_V2 succeeds instead of tearing down the whole
		// connection, but open neither a segment nor a decoder — write and
		// finalize already no-op when both are nil, so the payload is
		// silently discarded.
		return &fileIngest{stream: streamType}, nil

	default:
		// The server validates stream names before registering (06 §4), so this
		// is a programming error, not an agent one.
		return nil, errors.Errorf("no ingest pipeline for stream %q", streamType)
	}
}

// errGzipChannel marks a stream whose bytes open with the gzip magic. Channel
// gzip is off by default (ProtocolConst.ZIPPING_ENABLED = false); when a
// deployment turns it on the WHOLE channel is one gzip stream and the demuxer
// here would parse compressed garbage. Fail loudly instead (06 §7, №21).
var errGzipChannel = errors.New("stream begins with the gzip magic (0x1F 0x8B): channel gzip is not supported")

// gzipMagic is the two-byte header every gzip member starts with (RFC 1952).
var gzipMagic = []byte{0x1F, 0x8B}

// startDecoder runs one decode goroutine over a pipe. The decode func returns
// an error when it rejects the stream (malformed, or gzip-wrapped); the wrapper
// then CloseWithError the read end, so the connection goroutine's next write
// surfaces the failure and the server answers ACK_ERROR_MAGIC (06 §6, №21).
// After the decoder stops the pipe keeps draining so the connection goroutine
// can never block on a dead decoder.
func (pi *podIngest) startDecoder(ctx context.Context, stream string, seg *hotstore.Segment, decode func(io.Reader) error) *fileIngest {
	prd, pwr := io.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		// Sniff the first two bytes for the gzip magic, then feed the decoder the
		// original byte stream (head re-prepended) so it sees no gap.
		var head [2]byte
		n, readErr := io.ReadFull(prd, head[:])
		src := io.MultiReader(bytes.NewReader(head[:n]), prd)

		var err error
		switch {
		case readErr != nil && readErr != io.EOF && readErr != io.ErrUnexpectedEOF:
			err = readErr // a torn stream, not a short one
		case n == 2 && bytes.Equal(head[:], gzipMagic):
			err = errGzipChannel
		default:
			err = decode(src)
		}
		if err != nil {
			log.Error(ctx, err, "%s decoder of %v rejected the stream; asking the agent to resend", stream, pi.pr.Key)
			pi.l.noteDecoderError()
			_ = prd.CloseWithError(err)
		}
		_, _ = io.Copy(io.Discard, prd)
	}()
	return &fileIngest{stream: stream, seg: seg, pw: pwr, done: done}
}

// decodeTrace parses one trace file: the 8-byte timer epoch, then logical
// chunks delimited by EVENT_FINISH_RECORD, each indexed into
// chunk_index[threadId] (01 §4.3).
func decodeTrace(ctx context.Context, pr *hotstore.PodRestart, seg *hotstore.Segment, src io.Reader) {
	var header [8]byte
	if _, err := io.ReadFull(src, header[:]); err != nil {
		log.Error(ctx, err, "trace file of %v ended before the epoch header", pr.Key)
		return
	}
	pr.SetTimerStart(int64(binary.BigEndian.Uint64(header[:])))

	reader := pipe.NewPipeReader(io.MultiReader(bytes.NewReader(header[:]), src), false)
	for item := range pipe.TracesPipeReader(ctx, reader) {
		if !item.Complete {
			continue // a truncated tail chunk is never indexed
		}
		pr.AddChunk(seg, item.ThreadId, item.Offset, item.Size(), item.Time.UnixMilli())
	}
}
