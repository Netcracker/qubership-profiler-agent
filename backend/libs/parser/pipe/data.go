package pipe

import (
	"github.com/Netcracker/qubership-profiler-backend/libs/protocol/data"
	"time"
)

type (
	StringItem struct { // for sql and xml streams
		Id    int    // technical: ordinal number in a list
		Value string // actual string value
		pos   int64  // technical: position in bytes stream
	}
	DictionaryItem struct { // for dictionary streams
		Id    int    // dictionary id (aka tag_id)
		Value string // actual string value
		pos   int64  // technical: position in bytes stream
	}
	ParamItem struct { // for dictionary streams
		Id        int    // technical: ordinal number in a list
		Name      string // actual parameter name
		IsIndex   bool
		IsList    bool
		Order     int
		Signature string
		pos       int64 // technical: position in bytes stream
	}
	SuspendItem struct { // for dictionary streams
		id     int // ordinal number in a list
		Amount int // suspend time (ms)
		// Time is the pause END: the agent timestamps a stop-the-world delay
		// after detecting it (TimerCache dates[pos]=now), so the pause spans
		// [Time − Amount, Time]. Consumers store this end and build the interval
		// that way (SuspendLog.getSuspendDuration uses start = date − delay) (№4).
		Time  time.Time
		delta int   // technical: delta from prev number
		pos   int64 // technical: position in bytes stream
	}
	CallItem struct {
		id   int       // ordinal number in a list
		Time time.Time //
		Call data.Call // actual information
		pos  int64     // technical: position in bytes stream
	}
	TraceItem struct {
		Id       int       // ordinal number in a list
		Offset   int64     // position in bytes stream (part of foreign key)
		ThreadId uint64    // thread the chunk belongs to (chunk header field)
		Time     time.Time // offset time for trace block
		Complete bool      // true when the chunk ended with EVENT_FINISH_RECORD; false for a truncated tail
		Data     []byte    // binary data
		bytes    int       // count of trace binary data
		debug    string    // debug info (parsed trace's data)
	}
)

// Pos reports the byte offset of the record within the stream the reader consumed.
func (c CallItem) Pos() int64 {
	return c.pos
}

// Size reports the chunk length in bytes, including the 16-byte chunk header.
func (t TraceItem) Size() int {
	return t.bytes
}
