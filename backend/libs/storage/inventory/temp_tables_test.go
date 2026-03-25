package inventory

import (
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/common"
	model "github.com/Netcracker/qubership-profiler-backend/libs/storage"
)

func TestTempTableInfo(t *testing.T) {
	ts := time.Date(2024, 2, 21, 10, 2, 0, 0, time.UTC)
	tbi := createTempTable(ts)

	t.Run("update info", func(t *testing.T) {
		tbi.UpdateInfo(model.TableStatusPersisted, 102, 12, 1_231_234)
	})
}

func createTempTable(ts time.Time) *TempTableInfo {
	uuid := common.ToUuid(common.UUID{1: 4})
	tbi := &TempTableInfo{
		Uuid:           uuid,
		StartTime:      ts,
		EndTime:        ts.Add(15 * time.Minute),
		Status:         model.TableStatusCreating,
		Type:           model.TableCalls,
		TableName:      "table_1",
		CreatedTime:    time.Now(),
		RowsCount:      0,
		TableSize:      0,
		TableTotalSize: 0,
	}
	return tbi
}
