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

func (db *Client) GetPodsCount(ctx context.Context) (int64, error) {
	startTime := time.Now()
	log.Debug(ctx, "[GetPodsCount]")

	var count int64
	tx := db.db.Table(podTable).Count(&count)

	duration := time.Since(startTime)
	metrics.AddPgOperationMetricValue(metrics.EntityPod, metrics.PgOperationCount, duration, tx.RowsAffected, tx.Error != nil)

	if tx.Error != nil {
		log.Error(ctx, tx.Error, "Error getting pods count")
		return 0, tx.Error
	}

	log.Debug(ctx, "[GetPodsCount] finished. Found %d pods. Done in %v", count, duration)
	return count, nil
}

func (db *Client) GetPodById(ctx context.Context, id uuid.UUID) (*model.Pod, error) {
	startTime := time.Now()
	log.Debug(ctx, "[GetPodById] id %s", id)

	pod := model.Pod{}
	tx := db.db.Table(podTable).Where(model.Pod{
		Id: id,
	}).First(&pod)

	duration := time.Since(startTime)
	metrics.AddPgOperationMetricValue(metrics.EntityPod, metrics.PgOperationGetById, duration, tx.RowsAffected, tx.Error != nil)

	if tx.Error != nil {
		log.Error(ctx, tx.Error, "Error getting pod by id: id=%s", id)
		return nil, tx.Error
	}

	log.Debug(ctx, "[GetPodById] id %s. Done in %v", id, duration)
	return &pod, nil
}

func (db *Client) FindPod(ctx context.Context, namespace string, serviceName string, podName string) (*model.Pod, error) {
	startTime := time.Now()
	log.Debug(ctx, "[FindPod] namespace=%s, service name = %s, pod name = %s",
		namespace, serviceName, podName)

	pod := model.Pod{}
	tx := db.db.Table(podTable).Where(model.Pod{
		Namespace:   namespace,
		ServiceName: serviceName,
		PodName:     podName,
	}).First(&pod)

	duration := time.Since(startTime)
	metrics.AddPgOperationMetricValue(metrics.EntityPod, metrics.PgOperationSelectOne, duration, tx.RowsAffected, tx.Error != nil)

	if tx.Error != nil {
		log.Error(ctx, tx.Error, "Error finding pod: namespace=%s, service name = %s, pod name = %s",
			namespace, serviceName, podName)
		return nil, tx.Error
	}

	log.Debug(ctx, "[FindPod] namespace=%s, service name = %s, pod name = %s. Done in %v",
		namespace, serviceName, podName, duration)
	return &pod, nil
}

func (db *Client) SearchPods(ctx context.Context, podFilter model.PodFilter) ([]model.Pod, error) {
	startTime := time.Now()
	query := podFilter.SQLQuery()
	log.Debug(ctx, "[SearchPods] query=\"%s\"", query)

	pods := make([]model.Pod, 0)
	tx := db.db.Table(podTable).Find(&pods, query)

	duration := time.Since(startTime)
	metrics.AddPgOperationMetricValue(metrics.EntityPod, metrics.PgOperationSearchMany, duration, tx.RowsAffected, tx.Error != nil)

	if tx.Error != nil {
		log.Error(ctx, tx.Error, "Error searching pods with query=\"%s\"", query)
		return nil, tx.Error
	}

	log.Debug(ctx, "[SearchPods] with query=\"%s\" finished. found %d pods. Done in %v", query, len(pods), duration)
	return pods, nil
}

func (db *Client) RemoveOldPods(ctx context.Context, activeBefore time.Time) ([]model.Pod, error) {
	startTime := time.Now()
	log.Debug(ctx, "[RemoveOldPods] active before %v", activeBefore)

	pods := make([]model.Pod, 0)
	tx := db.db.Table(podTable).Model(&pods).Clauses(clause.Returning{}).
		Where("last_active < ?", activeBefore).Delete(&pods)

	duration := time.Since(startTime)
	metrics.AddPgOperationMetricValue(metrics.EntityPod, metrics.PgOperationRemove, duration, tx.RowsAffected, tx.Error != nil)

	if tx.Error != nil {
		log.Error(ctx, tx.Error, "Error removing pods active before %v", activeBefore)
		return nil, tx.Error
	}

	log.Debug(ctx, "[RemoveOldPods] active before %v, removed %d pods. Done in %v", activeBefore, len(pods), duration)
	return pods, nil
}
