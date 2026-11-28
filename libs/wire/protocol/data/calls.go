package data

import (
	"fmt"
)

type (
	Call struct {
		Time                LTime
		CpuTime             LTime
		WaitTime            LTime
		MemoryUsed          LTime
		Method              TagId
		Duration            LDuration
		NonBlocking         LTime
		QueueWaitDuration   LDuration
		SuspendDuration     LDuration
		Calls               LCounter
		TraceFileIndex      int
		BufferOffset        int
		ReactorFileIndex    int
		ReactorBufferOffset int
		RecordIndex         int
		Transactions        LCounter
		LogsGenerated       LBytes
		LogsWritten         LBytes
		FileRead            LBytes
		FileWritten         LBytes
		NetRead             LBytes
		NetWritten          LBytes
		ThreadName          string
		Params              map[TagId][]string
		Trace               []byte
		Namespace           string
		ServiceName         string
		PodName             string
		RestartTime         int64
	}
)

func (c Call) TraceIndex() string {
	return fmt.Sprintf("%d_%d_%d", c.TraceFileIndex, c.BufferOffset, c.RecordIndex)
}

func (c Call) String() string {
	return fmt.Sprintf("Call{time=%v, cpuTime=%v, waitTime=%v, memoryUsed=%v, "+
		"method=%v, duration=%v, queueWaitDuration=%v, suspendDuration=%v, "+
		"calls=%v, traceFileIndex=%v, bufferOffset=%v, recordIndex=%v, transactions=%v, "+
		"logsGenerated=%v, logsWritten=%v, fileRead=%v, fileWritten=%v, netRead=%v, netWritten=%v, threadName=%v, params=%v}",
		c.Time, c.CpuTime, c.WaitTime, c.MemoryUsed, c.Method, c.Duration, c.QueueWaitDuration, c.SuspendDuration,
		c.Calls, c.TraceFileIndex, c.BufferOffset, c.RecordIndex, c.Transactions,
		c.LogsGenerated, c.LogsWritten, c.FileRead, c.FileWritten, c.NetRead, c.NetWritten,
		c.ThreadName, c.Params)
}

func (c Call) Csv() string {
	return fmt.Sprintf("%8v; %7v; %7v; %8v; %6v; %7v; %7v; %7v; "+
		"%6v; %5v; %5v; %5v; %5v; %7v; %7v; %7v; %7v; %7v; %7v; %12v; %v",
		c.Time, c.CpuTime, c.WaitTime, c.MemoryUsed, c.Method, c.Duration, c.QueueWaitDuration, c.SuspendDuration,
		c.Calls, c.TraceFileIndex, c.BufferOffset, c.RecordIndex, c.Transactions,
		c.LogsGenerated, c.LogsWritten, c.FileRead, c.FileWritten, c.NetRead, c.NetWritten,
		c.ThreadName, c.Params)
}

func CallsCsvHeader() string {
	return fmt.Sprintf("#    n;     time; cpuTime;waitTime;memoryUsed;method;duration;queueWait;" +
		"suspend; calls;trFile;bufOffst;recordIdx; transactions; " +
		"logsGenerated; logsWritten; fileRead; fileWritten; netRead; netWritten; threadName; params\n")
}
