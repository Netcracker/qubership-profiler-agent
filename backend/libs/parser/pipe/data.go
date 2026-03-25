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
		id     int       // ordinal number in a list
		Amount int       // suspend time (ms)
		Time   time.Time // start time of suspend
		delta  int       // technical: delta from prev number
		pos    int64     // technical: position in bytes stream
	}
	CallItem struct {
		id   int       // ordinal number in a list
		Time time.Time //
		Call data.Call // actual information
		pos  int64     // technical: position in bytes stream
	}
	TraceItem struct {
		Id     int       // ordinal number in a list
		Offset int64     // position in bytes stream (part of foreign key)
		Time   time.Time // offset time for trace block
		Data   []byte    // binary data
		bytes  int       // count of trace binary data
		debug  string    // debug info (parsed trace's data)
	}
)
