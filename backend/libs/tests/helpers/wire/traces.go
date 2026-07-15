package wire

import "bytes"

// Trace event type bits, numerically identical to the agent's DumperConstants
// and to backend/libs/parser/pipe/traces.go.
const (
	eventEnter  = 0
	eventExit   = 1
	eventTag    = 2
	eventFinish = 3

	paramInline = 0
)

// TraceEvent is one event inside a logical trace chunk. DeltaMs is the
// event-time delta carried in the header byte (plus a varint continuation when
// it does not fit in five bits).
type TraceEvent struct {
	kind    byte
	DeltaMs int
	TagId   int    // ENTER: method id; TAG: param id
	Value   string // TAG only: inline value
}

// Enter encodes a method-enter event for the given dictionary method id.
func Enter(deltaMs, method int) TraceEvent {
	return TraceEvent{kind: eventEnter, DeltaMs: deltaMs, TagId: method}
}

// Exit encodes a method-exit event. Exits carry no tag id on the wire.
func Exit(deltaMs int) TraceEvent {
	return TraceEvent{kind: eventExit, DeltaMs: deltaMs}
}

// Tag encodes an inline-parameter tag event (PARAM_INLINE).
func Tag(deltaMs, tagId int, value string) TraceEvent {
	return TraceEvent{kind: eventTag, DeltaMs: deltaMs, TagId: tagId, Value: value}
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
			putTraceEvent(buf, e)
		}
		buf.WriteByte(eventFinish)
	}
	return buf.Bytes(), offsets
}

// putTraceEvent writes the header byte (type in bits 0-1, low five delta bits
// in bits 2-6, continuation flag in bit 7) and the type-specific payload.
func putTraceEvent(buf *bytes.Buffer, e TraceEvent) {
	header := e.kind | byte(e.DeltaMs&0x1f)<<2
	if e.DeltaMs > 0x1f {
		header |= 0x80
	}
	buf.WriteByte(header)
	if e.DeltaMs > 0x1f {
		putVarInt(buf, uint64(e.DeltaMs>>5))
	}
	if e.kind == eventExit {
		return // exits carry no tag id
	}
	putVarInt(buf, uint64(e.TagId))
	if e.kind == eventTag {
		buf.WriteByte(paramInline)
		putVarString(buf, e.Value)
	}
}
