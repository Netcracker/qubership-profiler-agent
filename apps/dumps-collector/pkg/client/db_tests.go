package db

import (
	"context"
	"fmt"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/metrics"
	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/model"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// TODO - move this file to cloud-storage
func (db *Client) CreatePodIfNotExist(ctx context.Context, namespace string, serviceName string, podName string, restartTime time.Time) (*model.Pod, bool, error) {
	startTime := time.Now()
	log.Debug(ctx, "[CreatePodIfNotExist] namespace=%s, service name = %s, pod name = %s, restart time = %v",
		namespace, serviceName, podName, restartTime)
	id, err := uuid.NewV7()
	if err != nil {
		log.Error(ctx, err, "Error generating new uuid for pod: namespace=%s, service name = %s, pod name = %s, restart time = %v",
			namespace, serviceName, podName, restartTime)
		return nil, false, err
	}
	pod := model.Pod{}
	tx := db.db.Table(podTable).Where(model.Pod{
		Namespace:   namespace,
		ServiceName: serviceName,
		PodName:     podName,
	}).Attrs(model.Pod{
		Id:          id,
		RestartTime: restartTime,
	}).FirstOrCreate(&pod)

	duration := time.Since(startTime)
	metrics.AddPgOperationMetricValue(metrics.EntityPod, metrics.PgOperationCreateOne, duration, tx.RowsAffected, tx.Error != nil)

	if tx.Error != nil {
		log.Error(ctx, tx.Error, "Error creaing new pod if not exist: namespace=%s, service name = %s, pod name = %s, restart time = %v",
			namespace, serviceName, podName, restartTime)
		return nil, false, tx.Error
	}

	log.Debug(ctx, "[CreatePodIfNotExist] namespace=%s, service name = %s, pod name = %s, restart time = %v, created %d pods. Done in %v",
		namespace, serviceName, podName, restartTime, tx.RowsAffected, duration)
	return &pod, tx.RowsAffected > 0, nil
}

func (db *Client) CreateHeapDumpIfNotExist(ctx context.Context, dump model.DumpInfo) (*model.HeapDump, bool, error) {
	startTime := time.Now()
	log.Debug(ctx, "[CreateHeapDumpIfNotExist] for pod name %s and creation time %v", dump.Pod.PodName, dump.CreationTime)

	if dump.DumpType != model.HeapDumpType {
		log.Error(ctx, nil, "Found unsupported dump type: %s", dump.DumpType)
		return nil, false, fmt.Errorf("found unsupported dump type: %s", dump.DumpType)
	}

	handle := fmt.Sprintf("%s-heap-%d", dump.Pod.PodName, dump.CreationTime.UnixMilli())
	heapDump := model.HeapDump{}
	tx := db.db.Table(heapDumpsTable).Where(model.HeapDump{
		Handle: handle,
	}).Attrs(model.HeapDump{
		PodId:        dump.Pod.Id,
		CreationTime: dump.CreationTime,
		FileSize:     dump.FileSize,
	}).FirstOrCreate(&heapDump)

	duration := time.Since(startTime)
	metrics.AddPgOperationMetricValue(metrics.EntityHeapDump, metrics.PgOperationCreateOne, duration, tx.RowsAffected, tx.Error != nil)

	if tx.Error != nil {
		log.Error(ctx, tx.Error, "Error creaing new heap dump if not exist: handle=%s", handle)
		return nil, false, tx.Error
	}

	log.Debug(ctx, "[CreateHeapDumpIfNotExist] for pod name %s and creation time %v finished, created %d dumps. Done in %v",
		dump.Pod.PodName, dump.CreationTime, tx.RowsAffected, duration)
	return &heapDump, tx.RowsAffected > 0, nil
}

func (db *Client) UpdatePodLastActive(ctx context.Context, namespace string, serviceName string, podName string, restartTime time.Time, lastActive time.Time) (*model.Pod, error) {
	startTime := time.Now()
	log.Debug(ctx, "[UpdatePodLastActive] namespace=%s, service name = %s, pod name = %s, restart time = %v, new last active = %v",
		namespace, serviceName, podName, restartTime, lastActive)

	pod := model.Pod{}
	tx := db.db.Table(podTable).Model(&pod).Clauses(clause.Returning{}).
		Where(model.Pod{
			Namespace:   namespace,
			ServiceName: serviceName,
			PodName:     podName,
			RestartTime: restartTime,
		}).
		Update("last_active", gorm.Expr("GREATEST(COALESCE(last_active, TO_TIMESTAMP(0)), ?)", lastActive))

	duration := time.Since(startTime)
	metrics.AddPgOperationMetricValue(metrics.EntityPod, metrics.PgOperationUpdate, duration, tx.RowsAffected, tx.Error != nil)

	if tx.Error != nil {
		log.Error(ctx, tx.Error, "Error updating pod: namespace=%s, service name = %s, pod name = %s, restart time = %v, new last active = %v",
			namespace, serviceName, podName, restartTime, lastActive)
		return nil, tx.Error
	}

	log.Debug(ctx, "[FindPod] namespace=%s, service name = %s, pod name = %s, restart time = %v, new last active = %v. Done in %v",
		namespace, serviceName, podName, restartTime, lastActive, duration)
	return &pod, nil
}

func (db *Client) InsertHeapDumps(ctx context.Context, dumps []model.DumpInfo) ([]model.HeapDump, error) {
	startTime := time.Now()
	log.Debug(ctx, "[InsertHeapDumps] dumps count %d", len(dumps))

	heapDumps := make([]model.HeapDump, len(dumps))

	for i, dump := range dumps {
		if dump.DumpType != model.HeapDumpType {
			log.Error(ctx, nil, "Found unsupported dump type: %s", dump.DumpType)
			return nil, fmt.Errorf("found unsupported dump type: %s", dump.DumpType)
		}
		heapDumps[i] = model.HeapDump{
			Handle:       fmt.Sprintf("%s-heap-%d", dump.Pod.PodName, dump.CreationTime.UnixMilli()),
			PodId:        dump.Pod.Id,
			CreationTime: dump.CreationTime,
			FileSize:     dump.FileSize,
		}
	}

	tx := db.db.Table(heapDumpsTable).Create(&heapDumps)

	duration := time.Since(startTime)
	metrics.AddPgOperationMetricValue(metrics.EntityHeapDump, metrics.PgOperationInsertMany, duration, tx.RowsAffected, tx.Error != nil)

	if tx.Error != nil {
		log.Error(ctx, tx.Error, "Error creating heap dumps")
		return nil, tx.Error
	}

	log.Debug(ctx, "[InsertHeapDumps] finished, inserted %d dumps. Done in %v", len(heapDumps), duration)
	return heapDumps, nil
}

func (db *dumpDbClientImpl) CreateTdTopDumpIfNotExist(ctx context.Context, dump model.DumpInfo) (*model.DumpObject, bool, error) {
	startTime := time.Now()
	tableName := db.DumpTable(dump.CreationTime)
	log.Debug(ctx, "[CreateTdTopDumpIfNotExist] for pod name %s, type %s and creation time %v", dump.Pod.PodName, dump.DumpType, dump.CreationTime)

	if dump.DumpType != model.TdDumpType && dump.DumpType != model.TopDumpType {
		log.Error(ctx, nil, "Found unsupported dump type: %s", dump.DumpType)
		return nil, false, fmt.Errorf("found unsupported dump type: %s", dump.DumpType)
	}

	id, err := uuid.NewV7()
	if err != nil {
		log.Error(ctx, err, "Error generating new uuid for dump")
		return nil, false, err
	}

	tdTopDump := model.DumpObject{}
	tx := db.db.Table(tableName).Where(model.DumpObject{
		PodId:        dump.Pod.Id,
		CreationTime: dump.CreationTime,
		DumpType:     dump.DumpType,
	}).Attrs(model.DumpObject{
		Id:       id,
		FileSize: dump.FileSize,
	}).FirstOrCreate(&tdTopDump)

	duration := time.Since(startTime)
	metrics.AddPgOperationMetricValue(metrics.EntityTdTopDump, metrics.PgOperationCreateOne, duration, tx.RowsAffected, tx.Error != nil)

	if tx.Error != nil {
		log.Error(ctx, tx.Error, "Error creaing new td/top dump if not exist: pod name %s, type %s and creation time %v",
			dump.Pod.PodName, dump.DumpType, dump.CreationTime)
		return nil, false, tx.Error
	}

	log.Debug(ctx, "[CreateTdTopDumpIfNotExist] for pod name %s, type %s and creation time %v finished, created %d dumps. Done in %v",
		dump.Pod.PodName, dump.DumpType, dump.CreationTime, tx.RowsAffected, duration)
	return &tdTopDump, tx.RowsAffected > 0, nil
}

func (db *dumpDbClientImpl) InsertTdTopDumps(ctx context.Context, tHour time.Time, dumps []model.DumpInfo) ([]model.DumpObject, error) {
	startTime := time.Now()
	tableName := db.dumpTableName
	log.Debug(ctx, "[InsertTdTopDumps] table name = %s, dumps count %d", tableName, len(dumps))

	tdTopDumps := make([]model.DumpObject, len(dumps))

	for i, dump := range dumps {
		if dump.DumpType != model.TdDumpType && dump.DumpType != model.TopDumpType {
			log.Error(ctx, nil, "Found unsupported dump type: %s", dump.DumpType)
			return nil, fmt.Errorf("found unsupported dump type: %s", dump.DumpType)
		}
		id, err := uuid.NewV7()
		if err != nil {
			log.Error(ctx, err, "Error generating new uuid for dump")
			return nil, err
		}
		tdTopDumps[i] = model.DumpObject{
			Id:           id,
			PodId:        dump.Pod.Id,
			CreationTime: dump.CreationTime,
			FileSize:     dump.FileSize,
			DumpType:     dump.DumpType,
		}
	}
	_, _, err := db.CreateTimelineIfNotExist(ctx, tHour)
	if err != nil {
		log.Error(ctx, err, "Error creating timelines for %s", tHour)
		return nil, err
	}

	tx := db.db.Table(tableName).Create(&tdTopDumps)

	duration := time.Since(startTime)
	metrics.AddPgOperationMetricValue(metrics.EntityTdTopDump, metrics.PgOperationInsertMany, duration, tx.RowsAffected, tx.Error != nil)

	if tx.Error != nil {
		log.Error(ctx, tx.Error, "Error creating tp/top dumps to table %s", tableName)
		return nil, tx.Error
	}

	log.Debug(ctx, "[InsertTdTopDumps] for table name %s finished, inserted %d dumps. Done in %v", tableName, len(tdTopDumps), duration)
	return tdTopDumps, nil
}

func (db *Client) CreateTimelineIfNotExist(ctx context.Context, t time.Time) (*model.Timeline, bool, error) {
	startTime := time.Now()
	log.Debug(ctx, "[CreateTimelineIfNotExist] time %v", t)

	timeHour := t.Truncate(Granularity)
	timeline := model.Timeline{}
	rowsAffected := int64(0)
	from := timeHour.Format("2006-01-02 15:04:05")
	to := timeHour.Add(time.Hour).Format("2006-01-02 15:04:05")

	err := db.db.Transaction(func(tx *gorm.DB) error {
		ttx := tx.Table(timelineTable).Where(model.Timeline{
			TsHour: timeHour,
		}).Attrs(model.Timeline{
			Status: model.RawStatus,
		}).FirstOrCreate(&timeline)

		rowsAffected = ttx.RowsAffected

		if ttx.Error != nil {
			log.Error(ctx, ttx.Error, "Error creating timeline %v", timeHour)
			return ttx.Error
		}

		sqlSchema := db.prepareSchemaQuery(db.dumpTableSchema,
			map[string]any{
				"TimeStamp": timeHour.UTC().Truncate(time.Hour).Unix(),
				"From":      fmt.Sprintf("('%s')", from),
				"To":        fmt.Sprintf("('%s')", to),
			},
		)

		if err := tx.Exec(sqlSchema).Error; err != nil {
			log.Error(ctx, err, "Error creating new temporary %s table for time %v", db.dumpTableName, timeHour)
			return err
		}

		return nil
	})

	duration := time.Since(startTime)
	metrics.AddPgOperationMetricValue(metrics.EntityTimelime, metrics.PgOperationCreateOne, duration, rowsAffected, err != nil)

	if err != nil {
		return nil, false, err
	}

	log.Debug(ctx, "[CreateTimelineIfNotExist] time %v finished. Done in %v", t, duration)
	return &timeline, rowsAffected > 0, nil
}
