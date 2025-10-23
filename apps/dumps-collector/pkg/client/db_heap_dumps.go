package db

import (
	"context"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/metrics"
	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/model"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"

	"github.com/google/uuid"
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

	log.Debug(ctx, "[FindHeapDump] handle = %s. Done in %v", handle, duration)
	return &heapDump, nil
}

func (db *Client) SearchHeapDumps(ctx context.Context, podIds []uuid.UUID, dateFrom time.Time, dateTo time.Time) ([]model.HeapDump, error) {
	startTime := time.Now()
	log.Debug(ctx, "[SearchHeapDumps] date from %v, date to %v, pod ids = %v", dateFrom, dateTo, podIds)

	heapDumps := make([]model.HeapDump, 0)
	tx := db.db.Table(heapDumpsTable).
		Where("pod_id In ? AND creation_time BETWEEN ? AND ?", podIds, dateFrom, dateTo).
		Find(&heapDumps)

	duration := time.Since(startTime)
	metrics.AddPgOperationMetricValue(metrics.EntityHeapDump, metrics.PgOperationSearchMany, duration, tx.RowsAffected, tx.Error != nil)

	if tx.Error != nil {
		log.Error(ctx, tx.Error, "Error searching heap dums: date from %v, date to %v, pod ids %v", dateFrom, dateTo, podIds)
		return nil, tx.Error
	}

	log.Debug(ctx, "[SearchHeapDumps] date from %v, date to %v, pods ids %v finished, found %d dumps. Done in %v",
		dateFrom, dateTo, podIds, len(heapDumps), duration)
	return heapDumps, nil
}

func (db *Client) RemoveOldHeapDumps(ctx context.Context, createdBefore time.Time) ([]model.HeapDump, error) {
	startTime := time.Now()
	log.Debug(ctx, "[RemoveOldHeapDumps] created before %v", createdBefore)

	heapDumps := make([]model.HeapDump, 0)

	tx := db.db.Table(heapDumpsTable).Model(&heapDumps).Clauses(clause.Returning{}).
		Where("creation_time < ?", createdBefore).Delete(&heapDumps)

	duration := time.Since(startTime)
	metrics.AddPgOperationMetricValue(metrics.EntityHeapDump, metrics.PgOperationRemove, duration, tx.RowsAffected, tx.Error != nil)

	if tx.Error != nil {
		log.Error(ctx, tx.Error, "Error removing heap dumps created before %v", createdBefore)
		return nil, tx.Error
	}

	log.Debug(ctx, "[RemoveOldHeapDumps] created before %v, removed %d dumps. Done in %v", createdBefore, len(heapDumps), duration)
	return heapDumps, nil
}

func (db *Client) TrimHeapDumps(ctx context.Context, limitPerPod int) ([]model.HeapDump, error) {
	startTime := time.Now()
	log.Debug(ctx, "[TrimHeapDumps] with limit %v", limitPerPod)

	heapDumps := make([]model.HeapDump, 0)

	tx := db.db.WithContext(ctx).
		Raw(`SELECT handle, pod_id, creation_time, file_size FROM trim_heap_dumps(?)`, limitPerPod).
		Scan(&heapDumps)

	duration := time.Since(startTime)
	metrics.AddPgOperationMetricValue(metrics.EntityHeapDump, metrics.PgOperationRemove, duration, tx.RowsAffected, tx.Error != nil)

	if tx.Error != nil {
		log.Error(ctx, tx.Error, "Error trim heap dumps with limit %v", limitPerPod)
		return nil, tx.Error
	}

	log.Debug(ctx, "[TrimHeapDumps] with limit %v, removed %d heap dumps from database. Done in %v",
		limitPerPod, len(heapDumps), duration)
	return heapDumps, nil
}
