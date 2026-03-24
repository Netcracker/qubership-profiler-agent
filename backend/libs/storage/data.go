package model

import (
	"fmt"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/common"
)

// -----------------------------------------------------------------------------

type CallWithTraces struct {
	Call
	Trace []byte
}

func (c *CallWithTraces) String() string {
	return fmt.Sprintf("%+v", *c)
}

// -----------------------------------------------------------------------------

type Call struct {
	Time              time.Time
	CpuTime           int64
	WaitTime          int64
	MemoryUsed        int64
	Duration          int32
	NonBlocking       int64
	QueueWaitDuration int32
	SuspendDuration   int32
	Calls             int32
	Transactions      int32
	LogsGenerated     int64
	LogsWritten       int64
	FileRead          int64
	FileWritten       int64
	NetRead           int64
	NetWritten        int64
	Namespace         string
	ServiceName       string
	PodName           string
	RestartTime       time.Time
	Method            int
	Params            map[int][]string
	TraceFileIndex    int
	BufferOffset      int
	RecordIndex       int
}

func (c *Call) String() string {
	return fmt.Sprintf("%+v", *c)
}

// -----------------------------------------------------------------------------

type Trace struct {
	PodName        string
	RestartTime    time.Time
	TraceFileIndex int
	BufferOffset   int
	RecordIndex    int
	Trace          []byte
}

func (t *Trace) String() string {
	return fmt.Sprintf("%+v", *t)
}

// -----------------------------------------------------------------------------

type Dump struct {
	UUID        common.Uuid
	CreatedTime time.Time
	Namespace   string
	ServiceName string
	PodName     string
	RestartTime time.Time
	PodType     PodType
	DumpType    DumpType
	BytesSize   int64
	Info        map[string]string
	BinaryData  []byte
}

func (d *Dump) String() string {
	return fmt.Sprintf("%+v", *d)
}
