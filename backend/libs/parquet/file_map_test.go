package parquet

import (
	"testing"
	"time"

	model "github.com/Netcracker/qubership-profiler-backend/libs/storage"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/stretchr/testify/assert"
)

func TestFileMap(t *testing.T) {
	ctx := log.SetLevel(log.Context("utest"), log.DEBUG)

	//ts := time.Date(2024, 2, 21, 10, 2, 0, 0, time.UTC)
	params := Params{
		RowGroupSize: 1000, PageSize: 100,
		CompressionType: CompressionGzip,
		BatchSize:       100,
		S3FileLifeTime:  24 * time.Hour,
	}

	fm := NewFileMap("../output/f_map", params)
	err := fm.ClearDirectory(ctx)
	assert.Nil(t, err)

	t.Run("file map", func(t *testing.T) {
		assert.Equal(t, 0, fm.Count())
		assert.Nil(t, fm.CloseAllFiles())

		fw, has := fm.Get("k1")
		assert.False(t, has)
		assert.Nil(t, fw)

		list, err := fm.ReadLocal(ctx)
		assert.Nil(t, err)
		assert.Equal(t, 0, len(list))

		assert.Equal(t, 0, fm.Count())
	})

	t.Run("calls file", func(t *testing.T) {
		dr := model.Durations.GetByName("10ms")
		assert.NotNil(t, dr)

		startTime := time.Now().Truncate(time.Hour)
		fw, err := fm.GetCallsFile(ctx, "ns1", dr, startTime)
		assert.Nil(t, err)
		assert.Equal(t, "ns1-10ms.parquet", fw.FileName)
		assert.Equal(t, int64(0), fw.FileSize)
		assert.Equal(t, startTime, fw.StartTime)
		assert.Equal(t, startTime.Add(params.S3FileLifeTime), fw.EndTime)

		fw, has := fm.Get("ns1-10ms")
		assert.True(t, has)
		assert.Equal(t, "ns1-10ms.parquet", fw.FileName)

		list, err := fm.ReadLocal(ctx)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(list))

		_, err = fm.GetCallsFile(ctx, "ns1", model.Durations.GetByName("invalid_duration"), startTime)
		assert.ErrorContains(t, err, "invalid duration range")

		assert.Equal(t, 1, fm.Count())

		assert.Nil(t, fm.CloseAllFiles())
	})

	t.Run("dumps file", func(t *testing.T) {
		dumpType := model.DumpTypeTop
		startTime := time.Now().Truncate(time.Hour)
		fw, err := fm.GetDumpsFile(ctx, "ns1", &dumpType, startTime)
		assert.Nil(t, err)
		assert.Equal(t, "ns1-top.parquet", fw.FileName)
		assert.Equal(t, int64(0), fw.FileSize)
		assert.Equal(t, startTime, fw.StartTime)
		assert.Equal(t, startTime.Add(params.S3FileLifeTime), fw.EndTime)

		fw, has := fm.Get("ns1-top")
		assert.True(t, has)
		assert.Equal(t, "ns1-top.parquet", fw.FileName)

		list, err := fm.ReadLocal(ctx)
		assert.Nil(t, err)
		assert.Equal(t, 2, len(list))

		_, err = fm.GetCallsFile(ctx, "ns1", model.Durations.GetByName("invalid_duration"), startTime)
		assert.ErrorContains(t, err, "invalid duration range")

		assert.Equal(t, 2, fm.Count())

		assert.Nil(t, fm.CloseAllFiles())
	})
}
