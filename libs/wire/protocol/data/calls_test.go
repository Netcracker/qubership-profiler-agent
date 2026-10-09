package data

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCall(t *testing.T) {
	c := generateCall()

	t.Run("trace index", func(t *testing.T) {
		assert.Equal(t, "1_8096_2", c.TraceIndex())
	})
	t.Run("string", func(t *testing.T) {
		assert.Equal(t, "Call{time=1698057132000, cpuTime=140, waitTime=1, memoryUsed=0, "+
			"method=104, duration=1230, queueWaitDuration=0, suspendDuration=0, "+
			"calls=10, traceFileIndex=1, bufferOffset=8096, recordIndex=2, transactions=20, "+
			"logsGenerated=1096502, logsWritten=1034004, fileRead=12303450, fileWritten=10000001, netRead=12303452, netWritten=10000003, "+
			"threadName=main, params=map[]}", c.String())
	})
	t.Run("csv", func(t *testing.T) {
		assert.Equal(t, "1698057132000;     140;       1;        0;    104;    1230;       0;       0;     10;     "+
			"1;  8096;     2;    20; "+
			"1096502; 1034004; 12303450; 10000001; 12303452; 10000003;         main; map[]", c.Csv())
	})
	t.Run("csv header", func(t *testing.T) {
		assert.Equal(t, "#    n;     time; cpuTime;waitTime;memoryUsed;method;duration;queueWait;suspend; "+
			"calls;trFile;bufOffst;recordIdx; transactions; "+
			"logsGenerated; logsWritten; fileRead; fileWritten; netRead; netWritten; threadName; params\n", CallsCsvHeader())
	})
}

func generateCall() Call {
	restart := time.Date(2023, 10, 23, 10, 30, 12, 0, time.UTC)
	ts := time.Date(2023, 10, 23, 10, 32, 12, 0, time.UTC)

	c := Call{
		Namespace:           "ns",
		ServiceName:         "svc",
		PodName:             "pod",
		RestartTime:         restart.UnixMilli(),
		Time:                ts.UnixMilli(),
		CpuTime:             140,
		WaitTime:            1,
		MemoryUsed:          0,
		Method:              104,
		Duration:            1230,
		NonBlocking:         0,
		QueueWaitDuration:   0,
		SuspendDuration:     0,
		Calls:               10,
		Transactions:        20,
		TraceFileIndex:      1,
		BufferOffset:        8096,
		RecordIndex:         2,
		ReactorFileIndex:    0,
		ReactorBufferOffset: 0,
		LogsGenerated:       1096502,
		LogsWritten:         1034004,
		FileRead:            12303450,
		FileWritten:         10000001,
		NetRead:             12303452,
		NetWritten:          10000003,
		ThreadName:          "main",
		Params:              map[int][]string{},
		Trace:               nil,
	}
	return c
}
