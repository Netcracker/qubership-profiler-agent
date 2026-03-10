package postgres

import (
	"context"
	"fmt"
	"strings"
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
	tdTopDump := model.DumpObject{}
	tx := db.db.Table(tableName).
		Where(model.DumpObject{
			PodId:        podId,
			CreationTime: creationTime,
			DumpType:     dumpType,
		}).First(&tdTopDump)

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

	// Double filtering by creation_time is used to cut off data by the current timeline (1 hour) and time-range
	// Example: from [2024-08-01 00:00:00 +0000 UTC] to [2024-08-01 00:59:59.999999999 +0000 UTC]
	var count int64
	tx := db.db.Table(tableName).
		Where("creation_time BETWEEN ? AND ? AND creation_time BETWEEN ? AND ?",
			dateFrom, dateTo, tHour, tHour.Add(Granularity-1)).Count(&count)

	duration := time.Since(startTime)
	metrics.AddPgOperationMetricValue(metrics.EntityTdTopDump, metrics.PgOperationCount, duration, tx.RowsAffected, tx.Error != nil)

	if tx.Error != nil {
		log.Error(ctx, tx.Error, "Error getting td/top dumps count from table name %s: date from %v, date to %v", tableName, dateFrom, dateTo)
		return 0, tx.Error
	}

	log.Debug(ctx, "[GetTdTopDumpsCount] for table name  %s, date from %v, date to %v finished. Found %d dumps. Done in %v",
		tableName, dateFrom, dateTo, count, duration)
	return count, nil
}

func (db *dumpDbClientImpl) SearchTdTopDumps(ctx context.Context, tHour time.Time, podIds []uuid.UUID, dateFrom time.Time, dateTo time.Time, dumpType model.DumpType) ([]model.DumpObject, error) {
	startTime := time.Now()
	tableName := db.dumpTableName
	log.Debug(ctx, "[SearchTdTopDumps] for table name  %s, pod ids = %v, dump type = %s, date from %v, date to %v",
		tableName, podIds, dumpType, dateFrom, dateTo)

	// Double filtering by creation_time is used to cut off data by the current timeline (1 hour) and time-range
	// Example: from [2024-08-01 00:00:00 +0000 UTC] to [2024-08-01 00:59:59.999999999 +0000 UTC]
	tdTopDumps := make([]model.DumpObject, 0)
	tx := db.db.Table(tableName).
		Where("pod_id IN ? AND dump_type = ? AND creation_time BETWEEN ? AND ? AND creation_time BETWEEN ? AND ?",
			podIds, dumpType, dateFrom, dateTo, tHour, tHour.Add(Granularity-1)).Find(&tdTopDumps)

	duration := time.Since(startTime)
	metrics.AddPgOperationMetricValue(metrics.EntityTdTopDump, metrics.PgOperationSearchMany, duration, tx.RowsAffected, tx.Error != nil)

	if tx.Error != nil {
		log.Error(ctx, tx.Error, "Error searching td/top dumps in table  %s: pod ids = %v,  dump type = %s, date from %v, date to %v",
			tableName, podIds, dumpType, dateFrom, dateTo)
		return nil, tx.Error
	}

	log.Debug(ctx, "[SearchTdTopDumps] in table %s: pod ids = %v,  dump type = %s, date from %v, date to %v finished, found %d dumps. Done in %v",
		tableName, podIds, dumpType, dateFrom, dateTo, len(tdTopDumps), duration)
	return tdTopDumps, nil
}

func (db *dumpDbClientImpl) CalculateSummaryTdTopDumps(ctx context.Context, tHour time.Time, podIds []uuid.UUID, dateFrom time.Time, dateTo time.Time) ([]model.DumpSummary, error) {
	startTime := time.Now()
	tableName := db.dumpTableName
	log.Debug(ctx, "[CalculateSummaryTdTopDumps] for table name  %s, date from %v, date to %v, timeline = %v, pod ids = %s",
		tableName, dateFrom, dateTo, tHour, podIds)

	// Double filtering by creation_time is used to cut off data by the current timeline (1 hour) and time-range
	// Example: from [2024-08-01 00:00:00 +0000 UTC] to [2024-08-01 00:59:59.999999999 +0000 UTC]
	summaries := make([]model.DumpSummary, 0)
	tx := db.db.Table(tableName).Select("pod_id",
		"MIN(creation_time) AS date_from",
		"MAX(creation_time) AS date_to",
		"SUM(file_size) AS sum_file_size").
		Where("pod_id IN ? AND creation_time BETWEEN ? AND ? AND creation_time BETWEEN ? AND ?",
			podIds, dateFrom, dateTo, tHour, tHour.Add(Granularity-1)).
		Group("pod_id").Find(&summaries)

	duration := time.Since(startTime)
	metrics.AddPgOperationMetricValue(metrics.EntityTdTopDump, metrics.PgOperationStatistic, duration, tx.RowsAffected, tx.Error != nil)

	if tx.Error != nil {
		log.Error(ctx, tx.Error, "Error calculating summary in table  %s: date from %v, date to %v, pod id = %s", tableName, dateFrom, dateTo, podIds)
		return nil, tx.Error
	}

	log.Debug(ctx, "[CalculateSummaryTdTopDumps] in table %s: date from %v, date to %v, pod ids = %s finished, calculated %d summaries. Done in %v",
		tableName, dateFrom, dateTo, podIds, len(summaries), duration)
	return summaries, nil
}

func (db *dumpDbClientImpl) RemoveOldTdTopDumps(ctx context.Context, tHour time.Time, createdBefore time.Time) ([]model.DumpObject, error) {
	startTime := time.Now()
	tableName := db.dumpTableName
	log.Debug(ctx, "[RemoveOldTdTopDumps] from table %s in %v hour created before %v", tableName, tHour, createdBefore)

	dumps := make([]model.DumpObject, 0)

	tx := db.db.Table(tableName).Model(&dumps).Clauses(clause.Returning{}).
		Where("creation_time < ?", createdBefore).Delete(&dumps)

	duration := time.Since(startTime)
	metrics.AddPgOperationMetricValue(metrics.EntityTdTopDump, metrics.PgOperationRemove, duration, tx.RowsAffected, tx.Error != nil)

	if tx.Error != nil {
		log.Error(ctx, tx.Error, "Error removing td/top dumps from table %s created before %v", tableName, createdBefore)
		return nil, tx.Error
	}

	log.Debug(ctx, "[RemoveOldTdTopDumps] from table %s created before %v, removed %d dumps. Done in %v", tableName, createdBefore, len(dumps), duration)
	return dumps, nil
}

func (db *Client) CreateTdTopDumpIfNotExist(ctx context.Context, dump model.DumpInfo) (*model.DumpObject, bool, error) {
	startTime := time.Now()
	log.Debug(ctx, "[CreateTdTopDumpIfNotExist] pod id = %s, creation time = %v, dump type = %s",
		dump.Pod.Id, dump.CreationTime, dump.DumpType)

	tdTopDump := model.DumpObject{}
	isCreated := false

	tableName := db.DumpTable(dump.CreationTime)

	err := db.db.Transaction(func(tx *gorm.DB) error {
		ttx := tx.Table(tableName).Where(model.DumpObject{
			PodId:        dump.Pod.Id,
			CreationTime: dump.CreationTime,
			DumpType:     dump.DumpType,
		}).FirstOrCreate(&tdTopDump, model.DumpObject{
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

func nullTimeToString(t *time.Time) string {
	if t == nil {
		return "NULL"
	}
	return fmt.Sprintf("'%s'", t.Format("2006-01-02 15:04:05"))
}

func formatDumpInfos(dumps []model.DumpInfo) string {
	var rows []string
	for _, dump := range dumps {
		row := fmt.Sprintf(
			"ROW(ROW('%s', '%s', '%s', '%s', '%s', %s), '%s', %d, '%s')",
			dump.Pod.Id, // uuid
			dump.Pod.Namespace,
			dump.Pod.ServiceName,
			dump.Pod.PodName,
			dump.Pod.RestartTime.Format("2006-01-02 15:04:05"),
			nullTimeToString(dump.Pod.LastActive),
			dump.CreationTime.Format("2006-01-02 15:04:05"),
			dump.FileSize,
			string(dump.DumpType),
		)
		rows = append(rows, row)
	}
	return "ARRAY[" + strings.Join(rows, ", ") + "]::dump_info[]"
}

func (db *dumpDbClientImpl) StoreDumpsTransactionally(ctx context.Context, heapDumps []model.DumpInfo, tdTopDumps []model.DumpInfo, tMinute time.Time) (model.StoreDumpResult, error) {
	startTime := time.Now()

	formattedHeapDumps := formatDumpInfos(heapDumps)
	formattedTdTopDumps := formatDumpInfos(tdTopDumps)

	query := `
		SELECT * FROM upsert_dumps_transactionally(?, ARRAY[` + formattedHeapDumps + `]::dump_info[], ARRAY[` + formattedTdTopDumps + `]::dump_info[])
	`
	var result model.StoreDumpResult
	tx := db.db.Raw(query, tMinute).Scan(&result)

	duration := time.Since(startTime)
	metrics.AddPgOperationMetricValue(metrics.EntityPod, metrics.PgOperationInsertMany, duration, result.PodsCreated, tx.Error != nil)
	metrics.AddPgOperationMetricValue(metrics.EntityTimelime, metrics.PgOperationInsertMany, duration, result.TimelinesCreated, tx.Error != nil)
	metrics.AddPgOperationMetricValue(metrics.EntityTdTopDump, metrics.PgOperationInsertMany, duration, result.TdTopDumpsInserted, tx.Error != nil)
	metrics.AddPgOperationMetricValue(metrics.EntityHeapDump, metrics.PgOperationInsertMany, duration, result.HeapDumpsInserted, tx.Error != nil)
	if tx.Error != nil {
		return model.StoreDumpResult{}, tx.Error
	}

	return result, nil
}
