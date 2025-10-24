package data

import (
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/protocol/data"
	model "github.com/Netcracker/qubership-profiler-backend/libs/storage"
	"github.com/Netcracker/qubership-profiler-backend/libs/storage/parquet"
)

// -----------------------------------------------------------------------------

type EnrichedCall struct {
	parquet.CallParquet
	// for postgres entity
	origin *data.Call
}

func (ec *EnrichedCall) GetPGCall(t time.Time) model.Call {
	return model.Call{
		Time:              t.Round(time.Millisecond),
		CpuTime:           ec.CpuTime,
		WaitTime:          ec.WaitTime,
		MemoryUsed:        ec.MemoryUsed,
		Duration:          ec.Duration,
		NonBlocking:       ec.NonBlocking,
		QueueWaitDuration: ec.QueueWaitDuration,
		SuspendDuration:   ec.SuspendDuration,
		Calls:             ec.Calls,
		Transactions:      int32(ec.Transactions), // TODO align types
		LogsGenerated:     int64(ec.LogsGenerated),
		LogsWritten:       int64(ec.LogsWritten),
		FileRead:          ec.FileRead,
		FileWritten:       ec.FileWritten,
		NetRead:           ec.NetRead,
		NetWritten:        ec.NetWritten,
		Namespace:         ec.Namespace,
		ServiceName:       ec.ServiceName,
		PodName:           ec.PodName,
		RestartTime:       time.Unix(0, ec.RestartTime*int64(time.Millisecond)),
		Method:            ec.origin.Method,
		Params:            ec.origin.Params,
		TraceFileIndex:    ec.origin.TraceFileIndex,
		BufferOffset:      ec.origin.BufferOffset,
		RecordIndex:       ec.origin.RecordIndex,
	}
}

func (ec *EnrichedCall) GetPGTrace() model.Trace {
	return model.Trace{
		PodName:        ec.PodName,
		RestartTime:    time.Unix(0, ec.RestartTime*int64(time.Millisecond)),
		TraceFileIndex: ec.origin.TraceFileIndex,
		BufferOffset:   ec.origin.BufferOffset,
		RecordIndex:    ec.origin.RecordIndex,
		Trace:          []byte(ec.Trace),
	}
}
