package wire

import "bytes"

// Trace event type bits, numerically identical to the agent's DumperConstants
// and to backend/libs/parser/pipe/traces.go.
const (
	eventEnter  = 0
	eventExit   = 1
	eventTag    = 2
	eventFinish = 3

	paramInline   = 0
	paramBig      = 1
	paramBigDedup = 1 | 2
)

// EventFinishRecord closes one logical trace chunk on the wire.
const EventFinishRecord = byte(eventFinish)

// TraceEvent is one event inside a logical trace chunk. DeltaMs is the
// event-time delta carried in the header byte (plus a varint continuation when
// it does not fit in five bits).
type TraceEvent struct {
	kind      byte
	paramType byte
	DeltaMs   int
	TagId     int    // ENTER: method id; TAG: param id
	Value     string // TAG only: inline value
	BigSeq    int    // TAG with a big-param reference: value-stream file index
	BigOffset int    // TAG with a big-param reference: offset within the file
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
	return TraceEvent{kind: eventTag, paramType: paramInline, DeltaMs: deltaMs, TagId: tagId, Value: value}
}

// BigTag encodes a big-parameter tag event: a (rolling_seq, offset) reference
// into the xml value stream for PARAM_BIG, or into the sql stream when dedup
// is set (PARAM_BIG_DEDUP; 01-write-contract.md §4.4).
func BigTag(deltaMs, tagId int, dedup bool, seq, offset int) TraceEvent {
	pt := byte(paramBig)
	if dedup {
		pt = paramBigDedup
	}
	return TraceEvent{kind: eventTag, paramType: pt, DeltaMs: deltaMs, TagId: tagId, BigSeq: seq, BigOffset: offset}
}

// PutTraceEvent writes the header byte (type in bits 0-1, low five delta bits
// in bits 2-6, continuation flag in bit 7) and the type-specific payload.
func PutTraceEvent(buf *bytes.Buffer, e TraceEvent) {
	header := e.kind | byte(e.DeltaMs&0x1f)<<2
	if e.DeltaMs > 0x1f {
		header |= 0x80
	}
	buf.WriteByte(header)
	if e.DeltaMs > 0x1f {
		PutVarInt(buf, uint64(e.DeltaMs>>5))
	}
	if e.kind == eventExit {
		return // exits carry no tag id
	}
	PutVarInt(buf, uint64(e.TagId))
	if e.kind == eventTag {
		buf.WriteByte(e.paramType)
		switch e.paramType {
		case paramBig, paramBigDedup:
			PutVarInt(buf, uint64(e.BigSeq))
			PutVarInt(buf, uint64(e.BigOffset))
		default:
			PutVarString(buf, e.Value)
		}
	}
}
