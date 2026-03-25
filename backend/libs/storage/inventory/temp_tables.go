package inventory

import (
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/common"
	model "github.com/Netcracker/qubership-profiler-backend/libs/storage"
)

type TempTableInfo struct { // not thread-safe!
	Uuid           common.Uuid
	StartTime      time.Time
	EndTime        time.Time
	Status         model.TableStatus
	Type           model.TableType
	TableName      string
	CreatedTime    time.Time
	RowsCount      int
	TableSize      int64
	TableTotalSize int64
}

func (tbi *TempTableInfo) UpdateInfo(status model.TableStatus, rowsCount int, size, totalSize int64) {
	tbi.Status = status
	tbi.RowsCount = rowsCount
	tbi.TableSize = size
	tbi.TableTotalSize = totalSize
}
