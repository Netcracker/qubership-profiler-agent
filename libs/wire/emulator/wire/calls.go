package wire

import (
	"bytes"
	"sort"
)

// CallRecord is one closed root call of the calls stream, version-4 format
// (Dumper.writeCall; decoded by libs/parser/pipe/calls.go). StartMs is
// absolute — CallsFileState derives the zig-zag delta the wire carries.
type CallRecord struct {
	StartMs        int64
	Method         int // dictionary tag id of the root method
	DurationMs     int
	ChildCalls     int
	ThreadName     string // registered in the per-file thread table on first use
	TraceFileIndex int    // agent stream-file index at the start of the call's trace bytes
	BufferOffset   int    // offset within TraceFileIndex where the call's first chunk begins
	RecordIndex    int    // event index of the root ENTER within that chunk
	Params         map[int][]string

	// The wire carries LogsWritten and (LogsGenerated − LogsWritten) as
	// unsigned varints, so LogsGenerated must be ≥ LogsWritten.
	LogsGenerated int64
	LogsWritten   int64

	CpuTimeMs  int64
	WaitTimeMs int64
	MemoryUsed int64

	FileRead    int64
	FileWritten int64
	NetRead     int64
	NetWritten  int64

	Transactions int
	QueueWaitMs  int
}

// CallsFileState is the per-file encoder state the agent keeps in CallsState:
// the thread-name table (a name is written inline on first use, referenced by
// index afterwards) and the running start-time base for the zig-zag deltas.
// Both reset on rotation, when a fresh file header carries a new base.
type CallsFileState struct {
	threadIdx map[string]int
	lastMs    int64
}

// NewCallsFileState starts the state of one calls file whose header carries
// baseMs.
func NewCallsFileState(baseMs int64) *CallsFileState {
	return &CallsFileState{threadIdx: map[string]int{}, lastMs: baseMs}
}

// PutRecord appends one record. Records are NOT decodable in isolation from
// the file: they depend on the running time base and the thread table.
func (st *CallsFileState) PutRecord(buf *bytes.Buffer, r CallRecord) {
	PutZigZag(buf, r.StartMs-st.lastMs)
	st.lastMs = r.StartMs
	PutVarInt(buf, uint64(r.Method))
	PutVarInt(buf, uint64(r.DurationMs))
	PutVarInt(buf, uint64(r.ChildCalls))
	idx, known := st.threadIdx[r.ThreadName]
	if !known {
		idx = len(st.threadIdx)
		st.threadIdx[r.ThreadName] = idx
	}
	PutVarInt(buf, uint64(idx))
	if !known {
		PutVarString(buf, r.ThreadName)
	}
	PutVarInt(buf, uint64(r.LogsWritten))
	PutVarInt(buf, uint64(r.LogsGenerated-r.LogsWritten))
	PutVarInt(buf, uint64(r.TraceFileIndex))
	PutVarInt(buf, uint64(r.BufferOffset))
	PutVarInt(buf, uint64(r.RecordIndex))
	PutVarInt(buf, uint64(r.CpuTimeMs)) // format >= 2
	PutVarInt(buf, uint64(r.WaitTimeMs))
	PutVarInt(buf, uint64(r.MemoryUsed))
	PutVarInt(buf, uint64(r.FileRead)) // format >= 3
	PutVarInt(buf, uint64(r.FileWritten))
	PutVarInt(buf, uint64(r.NetRead))
	PutVarInt(buf, uint64(r.NetWritten))
	PutVarInt(buf, uint64(r.Transactions)) // format >= 4
	PutVarInt(buf, uint64(r.QueueWaitMs))
	PutVarInt(buf, uint64(len(r.Params)))
	paramIds := make([]int, 0, len(r.Params))
	for id := range r.Params {
		paramIds = append(paramIds, id)
	}
	sort.Ints(paramIds) // deterministic bytes for a versioned generator
	for _, id := range paramIds {
		values := r.Params[id]
		PutVarInt(buf, uint64(id))
		PutVarInt(buf, uint64(len(values)))
		// The decoder fills the result slice from the highest index down,
		// so multi-value params are written in reverse.
		for i := len(values) - 1; i >= 0; i-- {
			PutVarString(buf, values[i])
		}
	}
}
