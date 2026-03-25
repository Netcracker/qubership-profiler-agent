package parquet

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestDumpParquet(t *testing.T) {
	ts := time.Date(2024, 2, 21, 10, 2, 0, 0, time.UTC)

	dp := &DumpParquet{
		Time:        ts.UnixMilli(),
		Namespace:   "ns",
		ServiceName: "svc",
		PodName:     "pod",
		RestartTime: ts.UnixMilli(),
		PodType:     "podType",
		DumpType:    "dumpType",
		BytesSize:   10_233_345,
		Info:        map[string]string{},
		BinaryData:  "BinaryData",
	}

	t.Run("dump", func(t *testing.T) {
		assert.Equal(t, "DumpParquet{time=1708509720000, "+
			"namespace=ns, service_name=svc, pod_namw=pod, restart_time=1708509720000, "+
			"pod_type=podType, dump_type=dumpType, bytes_size=10233345}", dp.String())
	})
}
