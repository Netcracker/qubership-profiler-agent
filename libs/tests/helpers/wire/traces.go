package wire

import (
	"bytes"

	emwire "github.com/Netcracker/qubership-profiler-backend/libs/emulator/wire"
)

// TraceEvent and its constructors live in libs/emulator/wire so the virtual
// dumper and these test builders share one encoder; the aliases below keep
// this package's call sites unchanged.
type TraceEvent = emwire.TraceEvent

// Enter encodes a method-enter event for the given dictionary method id.
func Enter(deltaMs, method int) TraceEvent { return emwire.Enter(deltaMs, method) }

// Exit encodes a method-exit event. Exits carry no tag id on the wire.
func Exit(deltaMs int) TraceEvent { return emwire.Exit(deltaMs) }

// Tag encodes an inline-parameter tag event (PARAM_INLINE).
func Tag(deltaMs, tagId int, value string) TraceEvent { return emwire.Tag(deltaMs, tagId, value) }

// BigTag encodes a big-parameter tag event: a (rolling_seq, offset) reference
// into the xml value stream for PARAM_BIG, or into the sql stream when dedup
// is set (PARAM_BIG_DEDUP; 01-write-contract.md §4.4).
func BigTag(deltaMs, tagId int, dedup bool, seq, offset int) TraceEvent {
	return emwire.BigTag(deltaMs, tagId, dedup, seq, offset)
}

// TraceChunk is one logical trace chunk: a 16-byte [threadId, startTime]
// header, the events of one thread, and a closing EVENT_FINISH_RECORD
// (backend/docs/design/01-write-contract.md §4.2).
type TraceChunk struct {
	ThreadId uint64
	StartMs  int64
	Events   []TraceEvent
}

// TraceStream encodes one trace stream file the way the agent writes it: the
// 8-byte timer epoch first (Dumper.initStreams fileRotated), then the chunks.
// It returns the encoded bytes plus the byte offset of every chunk within the
// file, so a test can point a Call's (buffer_offset, record_index) at a chunk's
// root ENTER.
func TraceStream(timerStartMs int64, chunks []TraceChunk) (data []byte, chunkOffsets []int64) {
	buf := &bytes.Buffer{}
	putFixedLong(buf, uint64(timerStartMs))

	offsets := make([]int64, 0, len(chunks))
	for _, c := range chunks {
		offsets = append(offsets, int64(buf.Len()))
		putFixedLong(buf, c.ThreadId)
		putFixedLong(buf, uint64(c.StartMs))
		for _, e := range c.Events {
			emwire.PutTraceEvent(buf, e)
		}
		buf.WriteByte(emwire.EventFinishRecord)
	}
	return buf.Bytes(), offsets
}

// ValueStream encodes one sql / xml value-stream file the way the agent
// writes it: a bare concatenation of var-strings with no file header
// (Dumper.initStreams gives the big-param streams no fileRotated content).
// It returns the byte offset of every value, so a test can point a BigTag's
// (rolling_seq, offset) reference at it.
func ValueStream(values []string) (data []byte, offsets []int64) {
	buf := &bytes.Buffer{}
	offsets = make([]int64, 0, len(values))
	for _, v := range values {
		offsets = append(offsets, int64(buf.Len()))
		putVarString(buf, v)
	}
	return buf.Bytes(), offsets
}
