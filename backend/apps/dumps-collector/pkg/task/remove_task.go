package task

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"time"

	db "github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/client"
	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/metrics"
	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/model"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
)

type RemoveTask struct {
	*task
}

func NewRemoveTask(baseDir string, dbClient db.DumpDbClient) (*RemoveTask, error) {
	task, err := newTask(baseDir, dbClient)
	if err != nil {
		return nil, err
	}

	metrics.AddTaskMetricValue(metrics.EntityTimelime, metrics.TaskRemove, 0, 0, false)
	metrics.AddTaskMetricValue(metrics.EntityPod, metrics.TaskRemove, 0, 0, false)
	metrics.AddTaskMetricValue(metrics.EntityHeapDump, metrics.TaskRemove, 0, 0, false)
	metrics.AddTaskMetricValue(metrics.EntityTdTopDump, metrics.TaskRemove, 0, 0, false)

	metrics.AddTaskMetricValue(metrics.EntityTimelime, metrics.TaskRemove, 0, 0, true)
	metrics.AddTaskMetricValue(metrics.EntityPod, metrics.TaskRemove, 0, 0, true)
	metrics.AddTaskMetricValue(metrics.EntityHeapDump, metrics.TaskRemove, 0, 0, true)
	metrics.AddTaskMetricValue(metrics.EntityTdTopDump, metrics.TaskRemove, 0, 0, true)
	return &RemoveTask{task: task}, nil
}

func (t *RemoveTask) Execute(ctx context.Context, tBefore time.Time) error {
	startTime := time.Now()
	log.Info(ctx, "Execute remove operation for time hours before %v", tBefore)

	timelines, err := t.dbClient.SearchTimelines(ctx, time.Time{}, tBefore)
	if err != nil {
		log.Error(ctx, err, "Error getting timelines from db")
		duration := time.Since(startTime)
		metrics.AddTaskMetricValue(metrics.EntityTimelime, metrics.TaskRemove, duration, 0, true)
		metrics.AddTaskMetricValue(metrics.EntityPod, metrics.TaskRemove, duration, 0, true)
		metrics.AddTaskMetricValue(metrics.EntityHeapDump, metrics.TaskRemove, duration, 0, true)
		metrics.AddTaskMetricValue(metrics.EntityTdTopDump, metrics.TaskRemove, duration, 0, true)
		return err
	}
	log.Info(ctx, "Found %d timelines to delete", len(timelines))

	for _, timeline := range timelines {
		startTime := time.Now()
		timelinesCount, podsCount, heapDumpsCount, tdTopDumpsCount, err := t.processTimeline(ctx, timeline)
		duration := time.Since(startTime)
		metrics.AddTaskMetricValue(metrics.EntityTimelime, metrics.TaskRemove, duration, timelinesCount, err != nil)
		metrics.AddTaskMetricValue(metrics.EntityPod, metrics.TaskRemove, duration, podsCount, err != nil)
		metrics.AddTaskMetricValue(metrics.EntityHeapDump, metrics.TaskRemove, duration, heapDumpsCount, err != nil)
		metrics.AddTaskMetricValue(metrics.EntityTdTopDump, metrics.TaskRemove, duration, tdTopDumpsCount, err != nil)

		if err == nil {
			metrics.RemoveActiveEntitiesMetricValue(metrics.EntityTimelime, timelinesCount)
			metrics.RemoveActiveEntitiesMetricValue(metrics.EntityPod, podsCount)
			metrics.RemoveActiveEntitiesMetricValue(metrics.EntityHeapDump, heapDumpsCount)
			metrics.RemoveActiveEntitiesMetricValue(metrics.EntityTdTopDump, tdTopDumpsCount)
		}
	}

	log.Info(ctx, "Remove operation for time hours before %v is finished", tBefore)
	return nil
}

func (t *RemoveTask) processTimeline(ctx context.Context, timeline model.Timeline) (timelinesCount int64, podsCount int64, heapDumpsCount int64, tdTopDumpsCount int64, err error) {
	log.Info(ctx, "Process remove timeline %v", timeline.TsHour)

	// Update timeline status
	_, err = t.dbClient.UpdateTimelineStatus(ctx, timeline.TsHour, model.RemovingStatus)
	if err != nil {
		log.Error(ctx, err, "Error setting status %s for timeline %v", model.RemovingStatus, timeline.TsHour)
		return 0, 0, 0, 0, err
	}

	// Remove files from PV
	pattern := filepath.Join(t.baseDir, "**", FileHourDirInPV(timeline.TsHour))
	files, err := filepath.Glob(pattern)
	if err != nil {
		log.Error(ctx, err, "Error getting time hours from PV")
		return 0, 0, 0, 0, err
	}

	patternZip := filepath.Join(t.baseDir, "**", FileHourZipInPV(timeline.TsHour))
	filesZip, err := filepath.Glob(patternZip)
	if err != nil {
		log.Error(ctx, err, "Error getting zipped time hours from PV")
		return 0, 0, 0, 0, err
	}
	files = append(files, filesZip...)

	for _, path := range files {
		if strings.HasSuffix(path, ".zip") {
			// Remove zip arcive
			if err := os.Remove(path); err != nil {
				log.Error(ctx, err, "Error removing zip archive %s", path)
				return 0, 0, 0, 0, err
			}
		} else {
			// Remove hour directory itself
			if err := os.RemoveAll(path); err != nil {
				log.Error(ctx, err, "Error removing directory %s", path)
				return 0, 0, 0, 0, err
			}
		}
		// remove parent directories if they are empty
		parentDir, _ := filepath.Split(path)
		parentDir = filepath.Clean(parentDir)
		for {
			// finish, if it's base directory
			if parentDir == t.baseDir {
				break
			}
			// finish if directory is not empty
			files, err := os.ReadDir(parentDir)
			if err != nil {
				log.Error(ctx, err, "Error getting files from directory %s", parentDir)
				break
			}
			if len(files) != 0 {
				break
			}
			// remove empty directory and go to parent
			if err := os.Remove(parentDir); err != nil {
				log.Error(ctx, err, "Error removing directory %s", parentDir)
				break
			}
			parentDir, _ = filepath.Split(parentDir)
			parentDir = filepath.Clean(parentDir)
		}
	}

	log.Info(ctx, "Files for hour %v are removed from PV", timeline.TsHour)

	err = t.dbClient.Transaction(ctx, func(tx db.DumpDbClient) error {
		// Remove heap dumps
		heapDumps, err := t.dbClient.RemoveOldHeapDumps(ctx, timeline.TsHour.Add(db.Granularity))
		if err != nil {
			log.Error(ctx, err, "Error removing heap dumps for hour %v", timeline.TsHour)
			return err
		}
		heapDumpsCount = int64(len(heapDumps))

		// Remove pods
		pods, err := t.dbClient.RemoveOldPods(ctx, timeline.TsHour.Add(db.Granularity))
		if err != nil {
			log.Error(ctx, err, "Error removing pods for hour %v", timeline.TsHour)
			return err
		}
		podsCount = int64(len(pods))

		// Get td/top dumps count form table (for metrics)
		tdTopDumpsCount, err = t.dbClient.GetTdTopDumpsCount(ctx, timeline.TsHour, time.Time{}, time.Now())
		if err != nil {
			log.Error(ctx, err, "Error getting td/top dumps for hour %v", timeline.TsHour)
			return err
		}

		// Remove timeline (and td/top dumps table)
		timelinesCount = 1
		if _, err := t.dbClient.RemoveTimeline(ctx, timeline.TsHour); err != nil {
			log.Error(ctx, err, "Error removing timeline %v", timeline.TsHour)
			return err
		}

		return nil
	})

	if err != nil {
		log.Error(ctx, err, "failed to remove information about timeline %v", timeline.TsHour)
	}

	log.Info(ctx, "Process remove timeline %v finished", timeline.TsHour)
	return
}
