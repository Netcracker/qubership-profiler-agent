package parquet

import (
	"testing"
	"time"

	model "github.com/Netcracker/qubership-profiler-backend/libs/storage"
	"github.com/Netcracker/qubership-profiler-backend/libs/storage/inventory"
	"github.com/Netcracker/qubership-profiler-backend/libs/storage/parquet"

	"github.com/Netcracker/qubership-profiler-backend/libs/common"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/stretchr/testify/assert"
)

func TestFileWorker_Close(t *testing.T) {
	ctx := log.SetLevel(log.Context("utest"), log.DEBUG)

	ts := time.Date(2024, 2, 21, 10, 2, 0, 0, time.UTC)
	params := Params{
		RowGroupSize: 1000, PageSize: 100,
		CompressionType: CompressionGzip,
		BatchSize:       100,
		S3FileLifeTime:  24 * time.Hour,
	}

	sfi := createDumpFile(ts)
	var obj interface{} = new(parquet.DumpParquet)

	fm, err := CreateWorker(ctx, params, sfi, obj)
	assert.Nil(t, err)

	t.Run("worker", func(t *testing.T) {

		t.Run("info", func(t *testing.T) {
			info := fm.Info()
			assert.Equal(t, model.FileCreating, info.Status)
		})

		t.Run("write", func(t *testing.T) {
			row := createDumpRow(ts)
			err := fm.Write(ctx, row)
			assert.Nil(t, err)
		})

		t.Run("stop", func(t *testing.T) {
			err := fm.WriteStop(ctx)
			assert.Nil(t, err)
		})

		t.Run("close", func(t *testing.T) {
			err := fm.Close(ctx)
			assert.Nil(t, err)
		})
	})
}

func createDumpRow(ts time.Time) *parquet.DumpParquet {
	dp := parquet.DumpParquet{
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
	return &dp
}

func createDumpFile(ts time.Time) *inventory.S3FileInfo {
	uuid := common.ToUuid(common.UUID{1: 4})
	startTime := ts.Truncate(time.Hour)
	sfi := inventory.PrepareDumpsFileInfo(uuid, ts, startTime, startTime.Add(time.Hour), "ns", model.DumpTypeTd,
		"nsw_dumps_td.parquet", "../output/nsw_dumps_td.parquet")
	return sfi
}
