package postgres

import (
	"context"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/metrics"
	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/model"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"

	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func (db *Client) GetHeapDumpsCount(ctx context.Context) (int64, error) {
	startTime := time.Now()
	log.Debug(ctx, "[GetHeapDumpsCount]")

	var count int64
	tx := db.db.Table(heapDumpsTable).Count(&count)

	duration := time.Since(startTime)
	metrics.AddPgOperationMetricValue(metrics.EntityHeapDump, metrics.PgOperationCount, duration, tx.RowsAffected, tx.Error != nil)

	if tx.Error != nil {
		log.Error(ctx, tx.Error, "Error getting heap dumps count")
		return 0, tx.Error
	}

	log.Debug(ctx, "[GetHeapDumpsCount] finished. Found %d heap dumps. Done in %v", count, duration)
	return count, nil
}

func (db *Client) FindHeapDump(ctx context.Context, handle string) (*model.HeapDump, error) {
	startTime := time.Now()
	log.Debug(ctx, "[FindHeapDump] handle = %s", handle)

	heapDump := model.HeapDump{}
	tx := db.db.Table(heapDumpsTable).
		Where(model.HeapDump{
			Handle: handle,
		}).First(&heapDump)

	duration := time.Since(startTime)
	metrics.AddPgOperationMetricValue(metrics.EntityHeapDump, metrics.PgOperationGetById, duration, tx.RowsAffected, tx.Error != nil)

	if tx.Error != nil {
		log.Error(ctx, tx.Error, "Error finding heap: handle=%s", handle)
		return nil, tx.Error
	}

	log.Debug(ctx, "[FindHeapDump] handle=%s finished. Done in %v", handle, duration)
	return &heapDump, nil
}

func (db *Client) CreateHeapDumpIfNotExist(ctx context.Context, dump model.DumpInfo) (*model.HeapDump, bool, error) {
	startTime := time.Now()
	log.Debug(ctx, "[CreateHeapDumpIfNotExist] pod id = %s, creation time = %v", dump.Pod.Id, dump.CreationTime)

	heapDump := model.HeapDump{}
	isCreated := false

	err := db.db.Transaction(func(tx *gorm.DB) error {
		handle := dump.GetHandle()
		ttx := tx.Table(heapDumpsTable).Where(model.HeapDump{
			Handle: handle,
		}).FirstOrCreate(&heapDump, model.HeapDump{
			Handle:       handle,
			PodId:        dump.Pod.Id,
			CreationTime: dump.CreationTime,
			FileSize:     dump.FileSize,
		})

		if ttx.Error != nil {
			log.Error(ctx, ttx.Error, "Error creating/getting heap dump: pod id = %s, creation time = %v",
				dump.Pod.Id, dump.CreationTime)
			return ttx.Error
		}

		isCreated = ttx.RowsAffected > 0
		return nil
	})

	duration := time.Since(startTime)
	metrics.AddPgOperationMetricValue(metrics.EntityHeapDump, metrics.PgOperationInsertOne, duration, 1, err != nil)

	if err != nil {
		return nil, false, err
	}

	log.Debug(ctx, "[CreateHeapDumpIfNotExist] pod id = %s, creation time = %v finished. Done in %v",
		dump.Pod.Id, dump.CreationTime, duration)
	return &heapDump, isCreated, nil
}

func (db *Client) InsertHeapDumps(ctx context.Context, dumps []model.DumpInfo) ([]model.HeapDump, error) {
	startTime := time.Now()
	log.Debug(ctx, "[InsertHeapDumps] %d dumps", len(dumps))

	heapDumps := make([]model.HeapDump, 0, len(dumps))
	for _, dump := range dumps {
		heapDumps = append(heapDumps, model.HeapDump{
			Handle:       dump.GetHandle(),
			PodId:        dump.Pod.Id,
			CreationTime: dump.CreationTime,
			FileSize:     dump.FileSize,
		})
	}

	tx := db.db.Table(heapDumpsTable).Clauses(clause.OnConflict{
		DoNothing: true,
	}).Create(&heapDumps)

	duration := time.Since(startTime)
	metrics.AddPgOperationMetricValue(metrics.EntityHeapDump, metrics.PgOperationInsertMany, duration, tx.RowsAffected, tx.Error != nil)

	if tx.Error != nil {
		log.Error(ctx, tx.Error, "Error inserting %d heap dumps", len(dumps))
		return nil, tx.Error
	}

	log.Debug(ctx, "[InsertHeapDumps] %d dumps finished. Done in %v", len(dumps), duration)
	return heapDumps, nil
}

func (db *Client) SearchHeapDumps(ctx context.Context, podIds []uuid.UUID, dateFrom time.Time, dateTo time.Time) ([]model.HeapDump, error) {
	startTime := time.Now()
	log.Debug(ctx, "[SearchHeapDumps] for pod ids = %v, date from %v, date to %v", podIds, dateFrom, dateTo)

	heapDumps := make([]model.HeapDump, 0)
	tx := db.db.Table(heapDumpsTable).
		Where("pod_id IN ? AND creation_time BETWEEN ? AND ?", podIds, dateFrom, dateTo).
		Find(&heapDumps)

	duration := time.Since(startTime)
	metrics.AddPgOperationMetricValue(metrics.EntityHeapDump, metrics.PgOperationSearchMany, duration, tx.RowsAffected, tx.Error != nil)

	if tx.Error != nil {
		log.Error(ctx, tx.Error, "Error searching heap dumps: pod ids = %v, date from %v, date to %v",
			podIds, dateFrom, dateTo)
		return nil, tx.Error
	}

	log.Debug(ctx, "[SearchHeapDumps] for pod ids = %v, date from %v, date to %v finished, found %d dumps. Done in %v",
		podIds, dateFrom, dateTo, len(heapDumps), duration)
	return heapDumps, nil
}

func (db *Client) RemoveOldHeapDumps(ctx context.Context, createdBefore time.Time) ([]model.HeapDump, error) {
	startTime := time.Now()
	log.Debug(ctx, "[RemoveOldHeapDumps] created before %v", createdBefore)

	dumps := make([]model.HeapDump, 0)
	tx := db.db.Table(heapDumpsTable).Model(&dumps).Clauses(clause.Returning{}).
		Where("creation_time < ?", createdBefore).Delete(&dumps)

	duration := time.Since(startTime)
	metrics.AddPgOperationMetricValue(metrics.EntityHeapDump, metrics.PgOperationRemove, duration, tx.RowsAffected, tx.Error != nil)

	if tx.Error != nil {
		log.Error(ctx, tx.Error, "Error removing heap dumps created before %v", createdBefore)
		return nil, tx.Error
	}

	log.Debug(ctx, "[RemoveOldHeapDumps] created before %v, removed %d dumps. Done in %v", createdBefore, len(dumps), duration)
	return dumps, nil
}

func (db *Client) TrimHeapDumps(ctx context.Context, limitPerPod int) ([]model.HeapDump, error) {
	startTime := time.Now()
	log.Debug(ctx, "[TrimHeapDumps] limit per pod = %d", limitPerPod)

	// Get all heap dumps that exceed the limit per pod
	query := `
		DELETE FROM heap_dumps
		WHERE handle IN (
			SELECT handle
			FROM (
				SELECT handle,
					ROW_NUMBER() OVER (PARTITION BY pod_id ORDER BY creation_time DESC) as rn
				FROM heap_dumps
			) t
			WHERE rn > ?
		)
		RETURNING *
	`

	dumps := make([]model.HeapDump, 0)
	tx := db.db.Raw(query, limitPerPod).Scan(&dumps)

	duration := time.Since(startTime)
	metrics.AddPgOperationMetricValue(metrics.EntityHeapDump, metrics.PgOperationRemove, duration, tx.RowsAffected, tx.Error != nil)

	if tx.Error != nil {
		log.Error(ctx, tx.Error, "Error trimming heap dumps with limit per pod = %d", limitPerPod)
		return nil, tx.Error
	}

	log.Debug(ctx, "[TrimHeapDumps] limit per pod = %d, removed %d dumps. Done in %v", limitPerPod, len(dumps), duration)
	return dumps, nil
}
