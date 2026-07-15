package hotstore

import (
	"encoding/binary"

	"github.com/Netcracker/qubership-profiler-backend/libs/parser/pipe"
	"github.com/pkg/errors"
)

type (
	// TraceEventKind mirrors the trace-stream event types
	// (backend/libs/parser/pipe/traces.go).
	TraceEventKind byte

	// TraceEvent is one decoded trace event inside a logical chunk. Payload
	// fields are populated per kind: TagId for ENTER and TAG, Value for inline
	// and indexed TAG params, BigSeq/BigOffset for PARAM_BIG / PARAM_BIG_DEDUP
	// references into the xml / sql value streams (01-write-contract.md §4.4).
	TraceEvent struct {
		Kind      TraceEventKind
		TagId     int
		ParamType byte
		Value     string
		BigSeq    int
		BigOffset int
	}
)

const (
	TraceEnter = TraceEventKind(pipe.EventEnterRecord)
	TraceExit  = TraceEventKind(pipe.EventExitRecord)
	TraceTag   = TraceEventKind(pipe.EventTagRecord)
)

// ParseChunk walks the events of one logical trace chunk: the 16-byte
// [threadId, startTime] header, then events to EVENT_FINISH_RECORD. visit is
// called with each event's index within the chunk — the axis a Call's
// record_index points along (01-write-contract.md §4.3) — and may return false
// to stop early. consumed reports the bytes read, including the finish marker
// on a complete parse, so a blob reader can step chunk by chunk (§4.5). Any
// structural truncation is an error: chunks enter the index only when their
// EVENT_FINISH_RECORD was parsed.
func ParseChunk(chunk []byte, visit func(index int, ev TraceEvent) bool) (threadId uint64, consumed int, err error) {
	if len(chunk) < 17 {
		return 0, 0, errors.New("chunk shorter than its 16-byte header")
	}
	threadId = binary.BigEndian.Uint64(chunk)

	pos := 16
	uvarint := func() (int, error) {
		v, n := binary.Uvarint(chunk[pos:])
		if n <= 0 {
			return 0, errors.Errorf("torn varint at chunk offset %d", pos)
		}
		pos += n
		return int(v), nil
	}

	for index := 0; ; index++ {
		if pos >= len(chunk) {
			return threadId, pos, errors.New("chunk has no EVENT_FINISH_RECORD")
		}
		header := chunk[pos]
		pos++
		kind := TraceEventKind(header & 0x3)
		if kind == TraceEventKind(pipe.EventFinishRecord) {
			return threadId, pos, nil
		}
		if header&0x80 != 0 { // event-time delta continuation
			if _, err := uvarint(); err != nil {
				return threadId, pos, err
			}
		}

		ev := TraceEvent{Kind: kind}
		if kind != TraceExit {
			tagId, err := uvarint()
			if err != nil {
				return threadId, pos, err
			}
			ev.TagId = tagId
		}
		if kind == TraceTag {
			if pos >= len(chunk) {
				return threadId, pos, errors.New("torn tag param type")
			}
			ev.ParamType = chunk[pos]
			pos++
			switch int(ev.ParamType) {
			case pipe.ParamInline, pipe.ParamIndex:
				runes, err := uvarint()
				if err != nil {
					return threadId, pos, err
				}
				if pos+2*runes > len(chunk) {
					return threadId, pos, errors.New("torn tag string value")
				}
				value := make([]rune, runes)
				for i := range value {
					// The agent writes UTF-16-ish 2-byte chars; mirror the pipe
					// reader's readChar (string(rune(int16))).
					value[i] = rune(int16(binary.BigEndian.Uint16(chunk[pos+2*i:])))
				}
				pos += 2 * runes
				ev.Value = string(value)
			case pipe.ParamBig, pipe.ParamBigDedup:
				var err error
				if ev.BigSeq, err = uvarint(); err != nil {
					return threadId, pos, err
				}
				if ev.BigOffset, err = uvarint(); err != nil {
					return threadId, pos, err
				}
			default:
				return threadId, pos, errors.Errorf("unknown tag param type %d", ev.ParamType)
			}
		}

		if !visit(index, ev) {
			return threadId, pos, nil
		}
	}
}
