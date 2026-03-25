package generator

import (
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/storage"

	"github.com/Netcracker/qubership-profiler-backend/libs/protocol/data"
)

func Convert(c data.Call) model.Call {
	call := model.Call{
		Time:              time.UnixMilli(c.Time),
		CpuTime:           c.CpuTime,
		WaitTime:          c.WaitTime,
		MemoryUsed:        c.MemoryUsed,
		Duration:          int32(c.Duration),
		NonBlocking:       c.NonBlocking,
		QueueWaitDuration: int32(c.QueueWaitDuration),
		SuspendDuration:   int32(c.SuspendDuration),
		Calls:             int32(c.Calls),
		Transactions:      int32(c.Transactions),
		LogsGenerated:     int64(c.LogsGenerated),
		LogsWritten:       int64(c.LogsWritten),
		FileRead:          int64(c.FileRead),
		FileWritten:       int64(c.FileWritten),
		NetRead:           int64(c.NetRead),
		NetWritten:        int64(c.NetWritten),
		Namespace:         c.Namespace,
		ServiceName:       c.ServiceName,
		PodName:           c.PodName,
		RestartTime:       time.UnixMilli(c.RestartTime),
		Method:            c.Method,
		Params:            c.Params,
		TraceFileIndex:    c.TraceFileIndex,
		BufferOffset:      c.BufferOffset,
		RecordIndex:       c.RecordIndex,
	}
	return call
}
