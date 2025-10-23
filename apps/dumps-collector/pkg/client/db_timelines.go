package db

import (
	"context"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/metrics"
	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/model"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func (db *Client) FindTimeline(ctx context.Context, t time.Time) (*model.Timeline, error) {
	startTime := time.Now()
	log.Debug(ctx, "[FindTimelineTable] time %v", t)

	timeHour := t.Truncate(Granularity)
	timeline := model.Timeline{}

	tx := db.db.Table(timelineTable).Where(model.Timeline{
		TsHour: timeHour,
	}).First(&timeline)

	duration := time.Since(startTime)
	metrics.AddPgOperationMetricValue(metrics.EntityTimelime, metrics.PgOperationGetById, duration, tx.RowsAffected, tx.Error != nil)

	if tx.Error != nil {
		log.Error(ctx, tx.Error, "Error finding timeline %v", t)
		return nil, tx.Error
	}

	log.Debug(ctx, "[FindTimelineTable] time %v finished. Done in %v", t, duration)
	return &timeline, nil
}

func (db *Client) SearchTimelines(ctx context.Context, dateFrom time.Time, dateTo time.Time) ([]model.Timeline, error) {
	startTime := time.Now()
	log.Debug(ctx, "[SearchTimelines] date from %v, date to %v", dateFrom, dateTo)

	timelines := make([]model.Timeline, 0, int(dateTo.Sub(dateFrom).Hours()))

	tx := db.db.Table(timelineTable).Where("ts_hour BETWEEN ? AND ? ORDER BY ts_hour DESC", dateFrom, dateTo).Find(&timelines)

	duration := time.Since(startTime)
	metrics.AddPgOperationMetricValue(metrics.EntityTimelime, metrics.PgOperationSearchMany, duration, tx.RowsAffected, tx.Error != nil)

	if tx.Error != nil {
		log.Error(ctx, tx.Error, "Error searching timeline date from %v, date to %v", dateFrom, dateTo)
		return nil, tx.Error
	}

	log.Debug(ctx, "[SearchTimelines] date from %v, date to %v finished. Done in %v", dateFrom, dateTo, duration)
	return timelines, nil
}

func (db *Client) UpdateTimelineStatus(ctx context.Context, t time.Time, status model.TimelineStatus) (*model.Timeline, error) {
	startTime := time.Now()
	log.Debug(ctx, "[UpdateTimelineStatus] for time %v, new status = %s", t, status)

	timeHour := t.Truncate(Granularity)
	timeline := model.Timeline{}

	tx := db.db.Table(timelineTable).Model(&timeline).Clauses(clause.Returning{}).
		Where(model.Timeline{
			TsHour: timeHour,
		}).
		Update("status", status)

	duration := time.Since(startTime)
	metrics.AddPgOperationMetricValue(metrics.EntityTimelime, metrics.PgOperationUpdate, duration, tx.RowsAffected, tx.Error != nil)

	if tx.Error != nil {
		log.Error(ctx, tx.Error, "Error updating timeline for time %v, new status = %s", t, status)
		return nil, tx.Error
	}

	log.Debug(ctx, "[UpdateTimelineStatus] for time %v, new status = %s finished. Done in %v", t, status, duration)
	return &timeline, nil
}

// RemoveTimeline We need to remove it because we need to use a maintenance job
func (db *Client) RemoveTimeline(ctx context.Context, t time.Time) (*model.Timeline, error) {
	startTime := time.Now()
	log.Debug(ctx, "[RemoveTimeline] time %v", t)

	timeHour := t.Truncate(Granularity)
	timeline := model.Timeline{}
	rowsAffected := int64(0)

	err := db.db.Transaction(func(tx *gorm.DB) error {
		ttx := tx.Table(timelineTable).Model(&timeline).Clauses(clause.Returning{}).
			Where(model.Timeline{
				TsHour: timeHour,
			}).Delete(&timeline)

		rowsAffected = ttx.RowsAffected

		if ttx.Error != nil {
			log.Error(ctx, ttx.Error, "Error deleting timeline %v", timeHour)
			return ttx.Error
		}

		dumpTable := db.DumpTable(t)
		if err := tx.Migrator().DropTable(dumpTable); err != nil {
			log.Error(ctx, err, "Error deleting temporary %s table for time %v", db.dumpTableName, timeHour)
			return err
		}

		return nil
	})

	duration := time.Since(startTime)
	metrics.AddPgOperationMetricValue(metrics.EntityTimelime, metrics.PgOperationRemove, duration, rowsAffected, err != nil)

	if err != nil {
		return nil, err
	}

	log.Debug(ctx, "[RemoveTimeline] time %v finished. Done in %v", t, time.Since(startTime))
	return &timeline, nil
}
