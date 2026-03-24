package maintenance

import (
	"context"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/Netcracker/qubership-profiler-backend/libs/storage"
)

const (
	PodInfoCapacity = 100
)

// TempTablesRemoveJob is a job to remove outdated temp tables
type MetadataRemoveJob struct {
	*MaintenanceJob
	toTs time.Time
}

// NewMetadataRemoveJob creates the new TempTablesRemoveJob
func NewMetadataRemoveJob(ctx context.Context, mJob *MaintenanceJob, ts time.Time) (*MetadataRemoveJob, error) {
	toTs := ts.Add(time.Duration(-mJob.JobConfig.MetadataRemoval) * time.Hour)
	log.Info(ctx, "Create new MetadataRemoveJob. Compaction time: %v", ts)

	return &MetadataRemoveJob{
		MaintenanceJob: mJob,
		toTs:           toTs,
	}, nil
}

// Execute is the main method for job
func (mrj *MetadataRemoveJob) Execute(ctx context.Context) error {
	startTime := time.Now()

	podsToRemove, err := mrj.getPodsToRemove(ctx)
	if err != nil {
		log.Error(ctx, err, "Error calculating pods that should be removed")
		return err
	}

	log.Debug(ctx, "Going to remove %d pods", len(podsToRemove))
	successfulPods := 0
	for _, pod := range podsToRemove {
		log.Debug(ctx, "Removing pod and pod restarts for pod with id %s", pod.PodId)
		if err := mrj.Postgres.RemovePodRestart(ctx, pod.PodId); err != nil {
			log.Error(ctx, err, "Error removing pod restarts for pod with id %s", pod.PodId)
			continue
		}
		// TODO: remove pod statistics?
		if err := mrj.Postgres.RemovePod(ctx, pod.PodId); err != nil {
			log.Error(ctx, err, "Error removing pod with id %s", pod.PodId)
			continue
		}
		successfulPods++
	}

	log.Info(ctx, "TempTablesRemoveJob for %v is finished. Removed %d pods. [Execution time - %v]", mrj.toTs, successfulPods, time.Since(startTime))
	return nil
}

func (mrj *MetadataRemoveJob) getPodsToRemove(ctx context.Context) ([]*model.PodInfo, error) {
	namespaces, err := mrj.Postgres.GetUniqueNamespaces(ctx)
	if err != nil {
		log.Error(ctx, err, "Error getting namespaces from PG")
		return nil, err
	}

	pods := make([]*model.PodInfo, 0, PodInfoCapacity)
	for _, ns := range namespaces {
		podsPerNs, err := mrj.Postgres.GetUniquePodsForNamespaceActiveBefore(ctx, ns, mrj.toTs)
		if err != nil {
			log.Error(ctx, err, "Error getting pods from namespace %s", ns)
		} else {
			pods = append(pods, podsPerNs...)
		}
	}
	return pods, nil
}
