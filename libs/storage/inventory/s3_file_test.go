package inventory

import (
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/common"
	model "github.com/Netcracker/qubership-profiler-backend/libs/storage"
	"github.com/stretchr/testify/assert"
)

func TestS3FileInfo(t *testing.T) {
	ts := time.Date(2024, 2, 21, 10, 2, 0, 0, time.UTC)

	t.Run("dumps", func(t *testing.T) {
		sfi := createDumpFile(ts)
		t.Run("add service", func(t *testing.T) {
			assert.NotNil(t, sfi.Services)
			assert.Equal(t, 0, sfi.Services.Size())

			services := map[string]any{"svc1": true, "svc4": true, "svc3": true}
			sfi.AddServices(services)

			assert.NotNil(t, sfi.Services)
			assert.Equal(t, 3, sfi.Services.Size())
			assert.Equal(t, []string{"svc1", "svc3", "svc4"}, sfi.Services.List())
		})

		t.Run("update local info", func(t *testing.T) {
			assert.Equal(t, model.FileCreating, sfi.Status)
			assert.Equal(t, 3, sfi.Services.Size())
			assert.Equal(t, int64(0), sfi.FileSize)

			sfi.UpdateLocalInfo([]string{"svc95", "svc99"}, 12_345_043)

			assert.Equal(t, model.FileCreated, sfi.Status)
			assert.Equal(t, 5, sfi.Services.Size())
			assert.Equal(t, []string{"svc1", "svc3", "svc4", "svc95", "svc99"}, sfi.Services.List())
			assert.Equal(t, int64(12_345_043), sfi.FileSize)
		})

		t.Run("update remote info", func(t *testing.T) {
			assert.Equal(t, "2024/02/21/10/ns_dumps_td.parquet", sfi.RemoteStoragePath)
			sfi.UpdateRemoteInfo(1000)
			assert.Equal(t, model.FileCompleted, sfi.Status)
			assert.Equal(t, int64(1000), sfi.FileSize)
		})
	})

	t.Run("calls", func(t *testing.T) {
		sfi := createCallsFile(ts)
		t.Run("add service", func(t *testing.T) {
			services := map[string]any{"svc1": true, "svc4": true, "svc3": true}
			assert.NotNil(t, sfi.Services)
			assert.Equal(t, 0, sfi.Services.Size())

			sfi.AddServices(services)
			assert.NotNil(t, sfi.Services)
			assert.Equal(t, 3, sfi.Services.Size())
			assert.Equal(t, []string{"svc1", "svc3", "svc4"}, sfi.Services.List())
		})

		t.Run("update local info", func(t *testing.T) {
			sfi.UpdateLocalInfo([]string{"svc5", "svc9"}, 12_345_043)
			assert.Equal(t, int64(12_345_043), sfi.FileSize)
		})

		t.Run("update remote info", func(t *testing.T) {
			assert.Equal(t, "2024/02/21/10/ns_calls_10.parquet", sfi.RemoteStoragePath)
			sfi.UpdateRemoteInfo(1000)
			assert.Equal(t, model.FileCompleted, sfi.Status)
			assert.Equal(t, int64(1000), sfi.FileSize)
		})
	})
}

func createCallsFile(ts time.Time) *S3FileInfo {
	uuid := common.ToUuid(common.UUID{1: 4})
	dr := model.Durations.GetByName("10ms")
	startTime := ts.Truncate(time.Hour)
	sfi := PrepareCallsFileInfo(uuid, ts, startTime, startTime.Add(time.Hour), "ns", dr,
		"ns_calls_10.parquet", "../../output/ns_calls_10.parquet")
	return sfi
}

func createDumpFile(ts time.Time) *S3FileInfo {
	uuid := common.ToUuid(common.UUID{1: 4})
	startTime := ts.Truncate(time.Hour)
	sfi := PrepareDumpsFileInfo(uuid, ts, startTime, startTime.Add(time.Hour), "ns", model.DumpTypeTd,
		"ns_dumps_td.parquet", "../../output/ns_dumps_td.parquet")
	return sfi
}
