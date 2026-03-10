package sqlite

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

func (db *dumpDbClientImpl) FindTdTopDump(ctx context.Context, podId uuid.UUID, creationTime time.Time, dumpType model.DumpType) (*model.DumpObject, error) {
	startTime := time.Now()
	log.Debug(ctx, "[FindTdTopDump] pod id = %s, creation time = %v, dump type = %s", podId, creationTime, dumpType)

	tableName := db.DumpTable(creationTime)
	
	// Ensure partition exists before querying
	if err := db.ensurePartitionExists(ctx, creationTime); err != nil {
		log.Error(ctx, err, "Error ensuring partition exists for time %v", creationTime)
		return nil, err
	}
	
	tdTopDump := model.DumpObject{}
	tx := db.db.Table(tableName).
		Where("pod_id = ? AND creation_time = ? AND dump_type = ?",
			podId.String(), creationTime, string(dumpType)).
		First(&tdTopDump)

	duration := time.Since(startTime)
	metrics.AddPgOperationMetricValue(metrics.EntityTdTopDump, metrics.PgOperationSelectOne, duration, tx.RowsAffected, tx.Error != nil)

	if tx.Error != nil {
		log.Error(ctx, tx.Error, "Error finding td/top dump: pod id = %v, creation time = %v, dump type = %s",
			podId, creationTime, dumpType)
		return nil, tx.Error
	}

	log.Debug(ctx, "[FindTdTopDump] pod id = %s, creation time = %v, dump type = %s finished. Done in %v",
		podId, creationTime, dumpType, duration)
	return &tdTopDump, nil
}

func (db *dumpDbClientImpl) GetTdTopDumpsCount(ctx context.Context, tHour time.Time, dateFrom time.Time, dateTo time.Time) (int64, error) {
	startTime := time.Now()
	tableName := db.dumpTableName
	log.Debug(ctx, "[GetTdTopDumpsCount] for table name %s, timeline %v, date from %v, date to %v", tableName, tHour, dateFrom, dateTo)

	// Ensure partition exists
	if err := db.ensurePartitionExists(ctx, tHour); err != nil {
		log.Error(ctx, err, "Error ensuring partition exists for time %v", tHour)
		return 0, err
	}
	
	actualTableName := db.DumpTable(tHour)

	// Double filtering by creation_time is used to cut off data by the current timeline (1 hour) and time-range
	var count int64
	tx := db.db.Table(actualTableName).
		Where("creation_time BETWEEN ? AND ? AND creation_time BETWEEN ? AND ?",
			dateFrom, dateTo, tHour, tHour.Add(Granularity-1)).Count(&count)

	duration := time.Since(startTime)
	metrics.AddPgOperationMetricValue(metrics.EntityTdTopDump, metrics.PgOperationCount, duration, tx.RowsAffected, tx.Error != nil)

	if tx.Error != nil {
		log.Error(ctx, tx.Error, "Error getting td/top dumps count from table name %s: date from %v, date to %v", actualTableName, dateFrom, dateTo)
		return 0, tx.Error
	}

	log.Debug(ctx, "[GetTdTopDumpsCount] for table name  %s, date from %v, date to %v finished. Found %d dumps. Done in %v",
		actualTableName, dateFrom, dateTo, count, duration)
	return count, nil
}

func (db *dumpDbClientImpl) SearchTdTopDumps(ctx context.Context, tHour time.Time, podIds []uuid.UUID, dateFrom time.Time, dateTo time.Time, dumpType model.DumpType) ([]model.DumpObject, error) {
	startTime := time.Now()
	tableName := db.dumpTableName
	log.Debug(ctx, "[SearchTdTopDumps] for table name  %s, pod ids = %v, dump type = %s, date from %v, date to %v",
		tableName, podIds, dumpType, dateFrom, dateTo)

	// Ensure partition exists
	if err := db.ensurePartitionExists(ctx, tHour); err != nil {
		log.Error(ctx, err, "Error ensuring partition exists for time %v", tHour)
		return nil, err
	}
	
	actualTableName := db.DumpTable(tHour)

	// Convert UUIDs to strings for SQLite
	podIdStrs := make([]string, len(podIds))
	for i, id := range podIds {
		podIdStrs[i] = id.String()
	}

	// Double filtering by creation_time is used to cut off data by the current timeline (1 hour) and time-range
	tdTopDumps := make([]model.DumpObject, 0)
	tx := db.db.Table(actualTableName).
		Where("pod_id IN ? AND dump_type = ? AND creation_time BETWEEN ? AND ? AND creation_time BETWEEN ? AND ?",
			podIdStrs, string(dumpType), dateFrom, dateTo, tHour, tHour.Add(Granularity-1)).Find(&tdTopDumps)

	duration := time.Since(startTime)
	metrics.AddPgOperationMetricValue(metrics.EntityTdTopDump, metrics.PgOperationSearchMany, duration, tx.RowsAffected, tx.Error != nil)

	if tx.Error != nil {
		log.Error(ctx, tx.Error, "Error searching td/top dumps in table  %s: pod ids = %v,  dump type = %s, date from %v, date to %v",
			actualTableName, podIds, dumpType, dateFrom, dateTo)
		return nil, tx.Error
	}

	log.Debug(ctx, "[SearchTdTopDumps] in table %s: pod ids = %v,  dump type = %s, date from %v, date to %v finished, found %d dumps. Done in %v",
		actualTableName, podIds, dumpType, dateFrom, dateTo, len(tdTopDumps), duration)
	return tdTopDumps, nil
}

func (db *dumpDbClientImpl) CalculateSummaryTdTopDumps(ctx context.Context, tHour time.Time, podIds []uuid.UUID, dateFrom time.Time, dateTo time.Time) ([]model.DumpSummary, error) {
	startTime := time.Now()
	tableName := db.dumpTableName
	log.Debug(ctx, "[CalculateSummaryTdTopDumps] for table name  %s, date from %v, date to %v, timeline = %v, pod ids = %s",
		tableName, dateFrom, dateTo, tHour, podIds)

	// Ensure partition exists
	if err := db.ensurePartitionExists(ctx, tHour); err != nil {
		log.Error(ctx, err, "Error ensuring partition exists for time %v", tHour)
		return nil, err
	}
	
	actualTableName := db.DumpTable(tHour)

	// Convert UUIDs to strings for SQLite
	podIdStrs := make([]string, len(podIds))
	for i, id := range podIds {
		podIdStrs[i] = id.String()
	}

	// Double filtering by creation_time is used to cut off data by the current timeline (1 hour) and time-range
	summaries := make([]model.DumpSummary, 0)
	tx := db.db.Table(actualTableName).Select("pod_id",
		"MIN(creation_time) AS date_from",
		"MAX(creation_time) AS date_to",
		"SUM(file_size) AS sum_file_size").
		Where("pod_id IN ? AND creation_time BETWEEN ? AND ? AND creation_time BETWEEN ? AND ?",
			podIdStrs, dateFrom, dateTo, tHour, tHour.Add(Granularity-1)).
		Group("pod_id").Find(&summaries)

	duration := time.Since(startTime)
	metrics.AddPgOperationMetricValue(metrics.EntityTdTopDump, metrics.PgOperationStatistic, duration, tx.RowsAffected, tx.Error != nil)

	if tx.Error != nil {
		log.Error(ctx, tx.Error, "Error calculating summary in table  %s: date from %v, date to %v, pod id = %s", actualTableName, dateFrom, dateTo, podIds)
		return nil, tx.Error
	}

	log.Debug(ctx, "[CalculateSummaryTdTopDumps] in table %s: date from %v, date to %v, pod ids = %s finished, calculated %d summaries. Done in %v",
		actualTableName, dateFrom, dateTo, podIds, len(summaries), duration)
	return summaries, nil
}

func (db *dumpDbClientImpl) RemoveOldTdTopDumps(ctx context.Context, tHour time.Time, createdBefore time.Time) ([]model.DumpObject, error) {
	startTime := time.Now()
	tableName := db.dumpTableName
	log.Debug(ctx, "[RemoveOldTdTopDumps] from table %s in %v hour created before %v", tableName, tHour, createdBefore)

	// Ensure partition exists
	if err := db.ensurePartitionExists(ctx, tHour); err != nil {
		log.Error(ctx, err, "Error ensuring partition exists for time %v", tHour)
		return nil, err
	}
	
	actualTableName := db.DumpTable(tHour)

	dumps := make([]model.DumpObject, 0)

	tx := db.db.Table(actualTableName).Model(&dumps).Clauses(clause.Returning{}).
		Where("creation_time < ?", createdBefore).Delete(&dumps)

	duration := time.Since(startTime)
	metrics.AddPgOperationMetricValue(metrics.EntityTdTopDump, metrics.PgOperationRemove, duration, tx.RowsAffected, tx.Error != nil)

	if tx.Error != nil {
		log.Error(ctx, tx.Error, "Error removing td/top dumps from table %s created before %v", actualTableName, createdBefore)
		return nil, tx.Error
	}

	log.Debug(ctx, "[RemoveOldTdTopDumps] from table %s created before %v, removed %d dumps. Done in %v", actualTableName, createdBefore, len(dumps), duration)
	return dumps, nil
}

func (db *Client) CreateTdTopDumpIfNotExist(ctx context.Context, dump model.DumpInfo) (*model.DumpObject, bool, error) {
	startTime := time.Now()
	log.Debug(ctx, "[CreateTdTopDumpIfNotExist] pod id = %s, creation time = %v, dump type = %s",
		dump.Pod.Id, dump.CreationTime, dump.DumpType)

	tdTopDump := model.DumpObject{}
	isCreated := false

	// Ensure partition exists
	if err := db.ensurePartitionExists(ctx, dump.CreationTime); err != nil {
		return nil, false, err
	}
	
	tableName := db.DumpTable(dump.CreationTime)

	err := db.db.Transaction(func(tx *gorm.DB) error {
		ttx := tx.Table(tableName).Where("pod_id = ? AND creation_time = ? AND dump_type = ?",
			dump.Pod.Id.String(), dump.CreationTime, string(dump.DumpType)).
			FirstOrCreate(&tdTopDump, model.DumpObject{
				Id:           uuid.New(),
				PodId:        dump.Pod.Id,
				CreationTime: dump.CreationTime,
				FileSize:     dump.FileSize,
				DumpType:     dump.DumpType,
			})

		if ttx.Error != nil {
			log.Error(ctx, ttx.Error, "Error creating/getting td/top dump: pod id = %s, creation time = %v, dump type = %s",
				dump.Pod.Id, dump.CreationTime, dump.DumpType)
			return ttx.Error
		}

		isCreated = ttx.RowsAffected > 0
		return nil
	})

	duration := time.Since(startTime)
	metrics.AddPgOperationMetricValue(metrics.EntityTdTopDump, metrics.PgOperationInsertOne, duration, 1, err != nil)

	if err != nil {
		return nil, false, err
	}

	log.Debug(ctx, "[CreateTdTopDumpIfNotExist] pod id = %s, creation time = %v, dump type = %s finished. Done in %v",
		dump.Pod.Id, dump.CreationTime, dump.DumpType, duration)
	return &tdTopDump, isCreated, nil
}

func (db *dumpDbClientImpl) InsertTdTopDumps(ctx context.Context, tHour time.Time, dumps []model.DumpInfo) ([]model.DumpObject, error) {
	startTime := time.Now()
	log.Debug(ctx, "[InsertTdTopDumps] %d dumps for timeline %v", len(dumps), tHour)

	// Ensure partition exists
	if err := db.ensurePartitionExists(ctx, tHour); err != nil {
		return nil, err
	}
	
	tableName := db.DumpTable(tHour)
	tdTopDumps := make([]model.DumpObject, 0, len(dumps))
	for _, dump := range dumps {
		tdTopDumps = append(tdTopDumps, model.DumpObject{
			Id:           uuid.New(),
			PodId:        dump.Pod.Id,
			CreationTime: dump.CreationTime,
			FileSize:     dump.FileSize,
			DumpType:     dump.DumpType,
		})
	}

	tx := db.db.Table(tableName).Clauses(clause.OnConflict{
		DoNothing: true,
	}).Create(&tdTopDumps)

	duration := time.Since(startTime)
	metrics.AddPgOperationMetricValue(metrics.EntityTdTopDump, metrics.PgOperationInsertMany, duration, tx.RowsAffected, tx.Error != nil)

	if tx.Error != nil {
		log.Error(ctx, tx.Error, "Error inserting %d td/top dumps for timeline %v", len(dumps), tHour)
		return nil, tx.Error
	}

	log.Debug(ctx, "[InsertTdTopDumps] %d dumps for timeline %v finished. Done in %v", len(dumps), tHour, duration)
	return tdTopDumps, nil
}

// StoreDumpsTransactionally implements the PostgreSQL stored procedure logic in Go
func (db *dumpDbClientImpl) StoreDumpsTransactionally(ctx context.Context, heapDumps []model.DumpInfo, tdTopDumps []model.DumpInfo, tMinute time.Time) (model.StoreDumpResult, error) {
	startTime := time.Now()
	log.Debug(ctx, "[StoreDumpsTransactionally] heap dumps: %d, td/top dumps: %d, time: %v",
		len(heapDumps), len(tdTopDumps), tMinute)

	result := model.StoreDumpResult{}
	
	err := db.db.Transaction(func(tx *gorm.DB) error {
		// Create a transactional client
		txClient := &dumpDbClientImpl{
			Client: Client{
				db:                 tx,
				schemas:            db.schemas,
				dumpTableName:      db.dumpTableName,
				partitionSchema:    db.partitionSchema,
				usedParams:         db.usedParams,
				partitionsMu:       db.partitionsMu,
				existingPartitions: db.existingPartitions,
			},
		}
		
		// Create timeline for the hour if not exists
		tHour := tMinute.Truncate(Granularity)
		timeline, timelineCreated, err := txClient.CreateTimelineIfNotExist(ctx, tHour)
		if err != nil {
			return err
		}
		if timelineCreated {
			result.TimelinesCreated = 1
		}
		
		// Ensure partition exists for this hour
		if err := txClient.ensurePartitionExists(ctx, tHour); err != nil {
			return err
		}
		
		// Track unique pods created
		createdPods := make(map[string]bool)
		
		// Process heap dumps
		for _, dump := range heapDumps {
			// Create or get pod
			pod, podCreated, err := txClient.CreatePodIfNotExist(ctx,
				dump.Pod.Namespace,
				dump.Pod.ServiceName,
				dump.Pod.PodName,
				dump.Pod.RestartTime)
			if err != nil {
				return err
			}
			if podCreated {
				podKey := pod.Id.String()
				if !createdPods[podKey] {
					createdPods[podKey] = true
					result.PodsCreated++
				}
			}
			
			// Update pod last active and dump type
			_, err = txClient.UpdatePodLastActive(ctx,
				dump.Pod.Namespace,
				dump.Pod.ServiceName,
				dump.Pod.PodName,
				dump.Pod.RestartTime,
				dump.CreationTime)
			if err != nil {
				return err
			}
			
			// Add dump type to pod
			if err := txClient.addDumpTypeToPod(ctx, pod.Id, dump.DumpType); err != nil {
				log.Error(ctx, err, "Warning: failed to update dump_type for pod %s", pod.Id)
				// Don't fail the transaction for this
			}
			
			// Insert heap dump
			handle := dump.GetHandle()
			heapDump := model.HeapDump{
				Handle:       handle,
				PodId:        pod.Id,
				CreationTime: dump.CreationTime,
				FileSize:     dump.FileSize,
			}
			
			if err := tx.Table(heapDumpsTable).Clauses(clause.OnConflict{
				DoNothing: true,
			}).Create(&heapDump).Error; err != nil {
				return err
			}
			result.HeapDumpsInserted++
		}
		
		// Process td/top dumps
		partitionTable := txClient.DumpTable(tHour)
		for _, dump := range tdTopDumps {
			// Create or get pod
			pod, podCreated, err := txClient.CreatePodIfNotExist(ctx,
				dump.Pod.Namespace,
				dump.Pod.ServiceName,
				dump.Pod.PodName,
				dump.Pod.RestartTime)
			if err != nil {
				return err
			}
			if podCreated {
				podKey := pod.Id.String()
				if !createdPods[podKey] {
					createdPods[podKey] = true
					result.PodsCreated++
				}
			}
			
			// Update pod last active and dump type
			_, err = txClient.UpdatePodLastActive(ctx,
				dump.Pod.Namespace,
				dump.Pod.ServiceName,
				dump.Pod.PodName,
				dump.Pod.RestartTime,
				dump.CreationTime)
			if err != nil {
				return err
			}
			
			// Add dump type to pod
			if err := txClient.addDumpTypeToPod(ctx, pod.Id, dump.DumpType); err != nil {
				log.Error(ctx, err, "Warning: failed to update dump_type for pod %s", pod.Id)
				// Don't fail the transaction for this
			}
			
			// Insert td/top dump
			tdTopDump := model.DumpObject{
				Id:           uuid.New(),
				PodId:        pod.Id,
				CreationTime: dump.CreationTime,
				FileSize:     dump.FileSize,
				DumpType:     dump.DumpType,
			}
			
			if err := tx.Table(partitionTable).Clauses(clause.OnConflict{
				DoNothing: true,
			}).Create(&tdTopDump).Error; err != nil {
				return err
			}
			result.TdTopDumpsInserted++
		}
		
		log.Info(ctx, "[StoreDumpsTransactionally] successfully stored: timelines=%d, pods=%d, heap_dumps=%d, td_top_dumps=%d",
			result.TimelinesCreated, result.PodsCreated, result.HeapDumpsInserted, result.TdTopDumpsInserted)
		
		return nil
	})
	
	duration := time.Since(startTime)
	metrics.AddPgOperationMetricValue(metrics.EntityPod, metrics.PgOperationInsertMany, duration, result.PodsCreated, err != nil)
	metrics.AddPgOperationMetricValue(metrics.EntityTimelime, metrics.PgOperationInsertMany, duration, result.TimelinesCreated, err != nil)
	metrics.AddPgOperationMetricValue(metrics.EntityTdTopDump, metrics.PgOperationInsertMany, duration, result.TdTopDumpsInserted, err != nil)
	metrics.AddPgOperationMetricValue(metrics.EntityHeapDump, metrics.PgOperationInsertMany, duration, result.HeapDumpsInserted, err != nil)
	
	if err != nil {
		log.Error(ctx, err, "[StoreDumpsTransactionally] failed")
		return model.StoreDumpResult{}, err
	}
	
	log.Debug(ctx, "[StoreDumpsTransactionally] completed in %v", duration)
	return result, nil
}
