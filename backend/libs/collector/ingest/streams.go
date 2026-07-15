package ingest

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"

	"github.com/Netcracker/qubership-profiler-backend/libs/collector/hotstore"
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
		return startDecoder(streamType, seg, func(prd *io.PipeReader) {
			decodeTrace(ctx, pr, seg, prd)
		}), nil

	case model.StreamSql, model.StreamXml:
		seg, err := pr.OpenSegment(streamType, agentFileIndex)
		if err != nil {
			return nil, err
		}
		// Offset-addressed value bytes: stored raw, never parsed on ingest.
		return &fileIngest{stream: streamType, seg: seg}, nil

	case model.StreamCalls:
		return startDecoder(streamType, nil, func(prd *io.PipeReader) {
			for item := range pipe.CallsPipeReader(ctx, pipe.NewPipeReader(prd, false)) {
				if err := pr.AppendCall(item.Time.UnixMilli(), item.Call); err != nil {
					log.Error(ctx, err, "index call of %v", pr.Key)
				}
			}
		}), nil

	case model.StreamDictionary:
		return startDecoder(streamType, nil, func(prd *io.PipeReader) {
			for item := range pipe.DictionaryPipeReader(ctx, pipe.NewPipeReader(prd, false), noPhraseLimit) {
				if _, err := pr.AppendDictionaryWord(item.Value); err != nil {
					log.Error(ctx, err, "append dictionary word of %v", pr.Key)
				}
			}
		}), nil

	case model.StreamParams:
		return startDecoder(streamType, nil, func(prd *io.PipeReader) {
			for item := range pipe.ParamsPipeReader(ctx, pipe.NewPipeReader(prd, false)) {
				err := pr.AppendParam(hotstore.ParamRecord{
					Name: item.Name, IsIndex: item.IsIndex, IsList: item.IsList,
					Order: item.Order, Signature: item.Signature,
				})
				if err != nil {
					log.Error(ctx, err, "append param of %v", pr.Key)
				}
			}
		}), nil

	case model.StreamSuspend:
		return startDecoder(streamType, nil, func(prd *io.PipeReader) {
			for item := range pipe.SuspendPipeReader(ctx, pipe.NewPipeReader(prd, false)) {
				if err := pr.AppendSuspend(item.Time.UnixMilli(), item.Amount); err != nil {
					log.Error(ctx, err, "append suspend of %v", pr.Key)
				}
			}
		}), nil

	default:
		// The server validates stream names before registering (06 §4), so this
		// is a programming error, not an agent one.
		return nil, errors.Errorf("no ingest pipeline for stream %q", streamType)
	}
}

// startDecoder runs one decode goroutine over a pipe. After the decoder stops
// — EOF or a malformed stream — the pipe keeps draining so the connection
// goroutine can never block on a dead decoder.
func startDecoder(stream string, seg *hotstore.Segment, decode func(*io.PipeReader)) *fileIngest {
	prd, pwr := io.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		decode(prd)
		_, _ = io.Copy(io.Discard, prd)
	}()
	return &fileIngest{stream: stream, seg: seg, pw: pwr, done: done}
}

// decodeTrace parses one trace file: the 8-byte timer epoch, then logical
// chunks delimited by EVENT_FINISH_RECORD, each indexed into
// chunk_index[threadId] (01 §4.3).
func decodeTrace(ctx context.Context, pr *hotstore.PodRestart, seg *hotstore.Segment, prd *io.PipeReader) {
	var header [8]byte
	if _, err := io.ReadFull(prd, header[:]); err != nil {
		log.Error(ctx, err, "trace file of %v ended before the epoch header", pr.Key)
		return
	}
	pr.SetTimerStart(int64(binary.BigEndian.Uint64(header[:])))

	reader := pipe.NewPipeReader(io.MultiReader(bytes.NewReader(header[:]), prd), false)
	for item := range pipe.TracesPipeReader(ctx, reader) {
		if !item.Complete {
			continue // a truncated tail chunk is never indexed
		}
		pr.AddChunk(seg, item.ThreadId, item.Offset, item.Size(), item.Time.UnixMilli())
	}
}
