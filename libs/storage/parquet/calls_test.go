package parquet

import (
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/storage/index"

	"github.com/Netcracker/qubership-profiler-backend/libs/common"
	"github.com/stretchr/testify/assert"
)

func TestCallParquet(t *testing.T) {
	uuid := common.UUID{1: 45}
	ts := time.Date(2024, 2, 21, 10, 2, 0, 0, time.UTC)

	c := &CallParquet{
		Time:        ts.UnixMilli(),
		CpuTime:     1026,
		Namespace:   "ns",
		ServiceName: "svc",
		PodName:     "pod",
		RestartTime: ts.UnixMilli(),
		Method:      "class.methodName()",
		Params:      Parameters{},
		TraceId:     "2_3409_0",
		Trace:       "traceBlob",
	}

	t.Run("idx", func(t *testing.T) {
		idx := index.NewMap(map[string]bool{"param1": true, "param2": true})
		fileUuid := common.ToUuid(uuid)

		c.AppendParamsToIndex(fileUuid, idx)
		assert.Equal(t, []string{}, idx.Parameters())

		c.Params.AddVal("param2", "val1")
		assert.Nil(t, c.Params.Get("param1"))
		assert.Equal(t, []string{"val1"}, c.Params.Get("param2"))

		c.AppendParamsToIndex(fileUuid, idx)
		assert.Equal(t, []string{"param2"}, idx.Parameters())

		c.Params.AddVal("param43", "val21", "val23")
		assert.Nil(t, c.Params.Get("param1"))
		assert.Equal(t, []string{"val1"}, c.Params.Get("param2"))
		assert.Equal(t, []string{"val21", "val23"}, c.Params.Get("param43"))

		c.AppendParamsToIndex(fileUuid, idx)
		assert.Equal(t, []string{"param2"}, idx.Parameters())
	})

	t.Run("string", func(t *testing.T) {
		s := c.String()
		assert.Equal(t, "CallParquet{time=1708509720000, cpuTime=1026, waitTime=0, memoryUsed=0,"+
			" method=class.methodName(), duration=0, queueWaitDuration=0, suspendDuration=0, calls=0, transactions=0, "+
			"traceId=2_3409_0, params=map[param2:[val1] param43:[val21 val23]]}", s)
	})
}
