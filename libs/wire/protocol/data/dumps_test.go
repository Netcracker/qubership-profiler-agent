package data

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDump_String(t *testing.T) {
	t.Run("string", func(t *testing.T) {
		d := generateDump()
		assert.Equal(t, "{Id:123 Namespace:ns ServiceName:svc PodName:pod DumpType:td "+
			"Time:1698057132000 Duration:0 BytesSize:132345430 ThreadCount:2 "+
			"BinaryData:nil}", d.String())
	})
}

func generateDump() *Dump {
	ts := time.Date(2023, 10, 23, 10, 32, 12, 0, time.UTC)

	return &Dump{
		Id:          123,
		Namespace:   "ns",
		ServiceName: "svc",
		PodName:     "pod",
		DumpType:    "td",
		Time:        ts.UnixMilli(),
		Duration:    0,
		BytesSize:   132_345_430,
		ThreadCount: 2,
		BinaryData:  "nil",
	}
}
