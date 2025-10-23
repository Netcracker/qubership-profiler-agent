package task

import (
	"context"
	"path/filepath"
	"sort"
	"time"

	db "github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/client"
	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/envconfig"
	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/metrics"
	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/model"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
)

type RescanTask struct {
	*task
}

func NewRescanTask(baseDir string, dbClient db.DumpDbClient) (*RescanTask, error) {
	task, err := newTask(baseDir, dbClient)
	if err != nil {
		return nil, err
	}
	metrics.AddTaskMetricValue(metrics.EntityTimelime, metrics.TaskRescan, 0, 0, false)
	metrics.AddTaskMetricValue(metrics.EntityPod, metrics.TaskRescan, 0, 0, false)
	metrics.AddTaskMetricValue(metrics.EntityHeapDump, metrics.TaskRescan, 0, 0, false)
	metrics.AddTaskMetricValue(metrics.EntityTdTopDump, metrics.TaskRescan, 0, 0, false)

	metrics.AddTaskMetricValue(metrics.EntityTimelime, metrics.TaskRescan, 0, 0, true)
	metrics.AddTaskMetricValue(metrics.EntityPod, metrics.TaskRescan, 0, 0, true)
	metrics.AddTaskMetricValue(metrics.EntityHeapDump, metrics.TaskRescan, 0, 0, true)
	metrics.AddTaskMetricValue(metrics.EntityTdTopDump, metrics.TaskRescan, 0, 0, true)
	return &RescanTask{task: task}, nil
}

func (t *RescanTask) Execute(ctx context.Context) error {
	startTime := time.Now()
	log.Info(ctx, "Execute rescan operation")

	podsCount := int64(0)
	heapDumpsCount := int64(0)
	tdTopDumpsCount := int64(0)
	existTimelines := make(map[time.Time]bool, 0)

	// Find all timelines that existed in DB before this rescan task was started
	err := t.dbClient.Transaction(ctx, func(tx db.DumpDbClient) error {
		timelines, err := t.dbClient.SearchTimelines(ctx, time.Time{}, time.Now())
		if err != nil {
			log.Error(ctx, err, "Error searching timelines")
			return err
		}
		for _, timeline := range timelines {
			existTimelines[timeline.TsHour] = true
		}

		podsCount, err = t.dbClient.GetPodsCount(ctx)
		if err != nil {
			log.Error(ctx, err, "Error getting pods count")
			return err
		}

		heapDumpsCount, err = t.dbClient.GetHeapDumpsCount(ctx)
		if err != nil {
			log.Error(ctx, err, "Error getting heap count")
			return err
		}

		for _, timeline := range timelines {
			count, err := t.dbClient.GetTdTopDumpsCount(ctx, timeline.TsHour, time.Time{}, time.Now())
			if err != nil {
				log.Error(ctx, err, "Error getting td/top dumps count for hour %v", timeline.TsHour)
				return err
			}
			tdTopDumpsCount += count
		}

		return nil
	})

	if err != nil {
		duration := time.Since(startTime)
		metrics.AddTaskMetricValue(metrics.EntityTimelime, metrics.TaskRescan, duration, 0, true)
		metrics.AddTaskMetricValue(metrics.EntityPod, metrics.TaskRescan, duration, 0, true)
		metrics.AddTaskMetricValue(metrics.EntityHeapDump, metrics.TaskRescan, duration, 0, true)
		metrics.AddTaskMetricValue(metrics.EntityTdTopDump, metrics.TaskRescan, duration, 0, true)
		return err
	}
	log.Info(ctx, "Found %d timelines in db, they will be excluded from rescan", len(existTimelines))

	metrics.AddActiveEntitiesMetricValue(metrics.EntityTimelime, int64(len(existTimelines)))
	metrics.AddActiveEntitiesMetricValue(metrics.EntityPod, podsCount)
	metrics.AddActiveEntitiesMetricValue(metrics.EntityHeapDump, heapDumpsCount)
	metrics.AddActiveEntitiesMetricValue(metrics.EntityTdTopDump, tdTopDumpsCount)

	// Retrieve timestamps of hours for which timelines exist in the database.
	tHours, err := t.collectHours(ctx)
	if err != nil {
		duration := time.Since(startTime)
		metrics.AddTaskMetricValue(metrics.EntityTimelime, metrics.TaskRescan, duration, 0, true)
		metrics.AddTaskMetricValue(metrics.EntityPod, metrics.TaskRescan, duration, 0, true)
		metrics.AddTaskMetricValue(metrics.EntityHeapDump, metrics.TaskRescan, duration, 0, true)
		metrics.AddTaskMetricValue(metrics.EntityTdTopDump, metrics.TaskRescan, duration, 0, true)
		return err
	}

	for _, tHour := range tHours {
		startTime := time.Now()

		// Skip all hours for which timelines already existed in DB before this rescan task started
		if _, found := existTimelines[tHour]; found {
			log.Info(ctx, "Hour %v already exists in DB, skip it", tHour)
			duration := time.Since(startTime)
			metrics.AddTaskMetricValue(metrics.EntityTimelime, metrics.TaskRescan, duration, 0, err != nil)
			metrics.AddTaskMetricValue(metrics.EntityPod, metrics.TaskRescan, duration, 0, err != nil)
			metrics.AddTaskMetricValue(metrics.EntityHeapDump, metrics.TaskRescan, duration, 0, err != nil)
			metrics.AddTaskMetricValue(metrics.EntityTdTopDump, metrics.TaskRescan, duration, 0, err != nil)
			continue
		}

		// Collect information about dumps for the specified hour and store their metadata in the database
		timelinesCount, podsCount, heapDumpsCount, tdTopDumpsCount, err := t.processHour(ctx, tHour)
		duration := time.Since(startTime)
		metrics.AddTaskMetricValue(metrics.EntityTimelime, metrics.TaskRescan, duration, timelinesCount, err != nil)
		metrics.AddTaskMetricValue(metrics.EntityPod, metrics.TaskRescan, duration, podsCount, err != nil)
		metrics.AddTaskMetricValue(metrics.EntityHeapDump, metrics.TaskRescan, duration, heapDumpsCount, err != nil)
		metrics.AddTaskMetricValue(metrics.EntityTdTopDump, metrics.TaskRescan, duration, tdTopDumpsCount, err != nil)

		if err == nil {
			metrics.AddActiveEntitiesMetricValue(metrics.EntityTimelime, timelinesCount)
			metrics.AddActiveEntitiesMetricValue(metrics.EntityPod, podsCount)
			metrics.AddActiveEntitiesMetricValue(metrics.EntityHeapDump, heapDumpsCount)
			metrics.AddActiveEntitiesMetricValue(metrics.EntityTdTopDump, tdTopDumpsCount)
		}
	}

	log.Info(ctx, "Rescan operation is finished")
	return nil
}

// collectHours returns a slice of unique hourly timestamps that have dumps, sorted in descending order.
//
// Example result:
//
//	[
//		2024-08-01 00:00:00 +0000 UTC,
//	 	2024-07-31 23:00:00 +0000 UTC,
//	 	2024-07-31 22:00:00 +0000 UTC
//	]
func (t *RescanTask) collectHours(ctx context.Context) ([]time.Time, error) {
	log.Info(ctx, "Collect hour directories in PV")

	// e.g., "/output/**/**/**/**/**"
	pattern := filepath.Join(t.baseDir, "**", "**", "**", "**", "**")

	/*
		Find all folders and archives for each hour
		e.g., files = [
			"output/test-namespace-1/2024/07/31/22",
			"output/test-namespace-1/2024/07/31/22.zip",
			"output/test-namespace-1/2024/07/31/23",
			"output/test-namespace-1/2024/08/01/00"
		]
	*/
	files, err := filepath.Glob(pattern)
	if err != nil {
		log.Error(ctx, err, "Error getting time hours from PV")
		return nil, err
	}

	tHours := make([]time.Time, 0, envconfig.EnvConfig.DeleteDays*24)

	for _, file := range files {

		// e.g., output/test-namespace-1/2024/07/31/22 -> test-namespace-1/2024/07/31/22
		path, err := t.splitBaseDirFromPath(file)
		if err != nil {
			log.Error(ctx, err, "Error calculating relative path for file %s from PV", file)
		}

		// e.g., test-namespace-1/2024/07/31/22 -> 2024-07-31 22:00:00 +0000
		tHour, err := ParseTimeHour(path)
		if err != nil {
			log.Error(ctx, err, "Error parsing hour from directory %s", path)
			return []time.Time{}, err
		}
		exist := false
		for _, existHour := range tHours {
			if existHour.Equal(*tHour) {
				exist = true
				break
			}
		}
		if !exist {
			tHours = append(tHours, *tHour)
		}
	}

	// Sort the hours in descending order
	sort.Slice(tHours, func(i, j int) bool {
		return tHours[i].After(tHours[j])
	})

	log.Info(ctx, "Found %d time hours in PV", len(tHours))
	return tHours, nil
}

// processHour collects information about dumps for the specified hour and stores their metadata in the database.
func (t *RescanTask) processHour(ctx context.Context, tHour time.Time) (timelinesCount int64, podsCount int64, heapDumpsCount int64, tdTopDumpsCount int64, err error) {
	log.Info(ctx, "Process rescan for hour %s", tHour)

	tdTopDumpsInfo, heapDumpsInfo, timelineStatus := t.collectDumpsForHour(ctx, tHour)
	timelinesCount, podsCount, heapDumpsCount, tdTopDumpsCount, err = t.storeDumps(ctx, tHour, timelineStatus, tdTopDumpsInfo, heapDumpsInfo)
	return
}

// collectDumpsForHour collects dump metadata for the given hour, searching both raw and zipped files.
//
// Returns:
//   - a slice of td/top dump info
//   - a slice of heap dump info
//   - a timeline status (RawStatus, ZippedStatus, or RemovingStatus)
func (t *RescanTask) collectDumpsForHour(ctx context.Context, tHour time.Time) ([]model.DumpInfo, []model.DumpInfo, model.TimelineStatus) {
	// Collect not zipped files
	pattern := filepath.Join(t.baseDir, "**", FileHourDirInPV(tHour), "**", "**", "**", "*.*")
	tdTopDumpsInfo, heapDumpsInfo := t.collectDumpsFromPattern(ctx, pattern)
	status := model.RawStatus

	// If no  not zipped files, collect zipped files
	if len(tdTopDumpsInfo) == 0 {
		status = model.ZippedStatus
		zipPattern := filepath.Join(t.baseDir, "**", FileHourZipInPV(tHour))
		files, err := filepath.Glob(zipPattern)
		if err != nil {
			log.Error(ctx, err, "Error getting zip files for %s", pattern)
			return tdTopDumpsInfo, heapDumpsInfo, status
		}
		for _, file := range files {
			newTdTopDumpsInfo, newHeapDumpsInfo, _ := t.collectDumpsFromZip(ctx, file)
			tdTopDumpsInfo = append(tdTopDumpsInfo, newTdTopDumpsInfo...)
			heapDumpsInfo = append(heapDumpsInfo, newHeapDumpsInfo...)
		}

		if len(tdTopDumpsInfo) == 0 && len(heapDumpsInfo) == 0 {
			status = model.RemovingStatus
		}
	}
	log.Info(ctx, "Successfully parsed %d td/top dumps and %d heap dumps for hour %v, result timeline status %s", len(tdTopDumpsInfo), len(heapDumpsInfo), tHour, status)
	return tdTopDumpsInfo, heapDumpsInfo, status
}

// storeDumps saves metadata about dumps to the database.
func (t *RescanTask) storeDumps(ctx context.Context, tHour time.Time, status model.TimelineStatus, tdTopDumps []model.DumpInfo, heapDumps []model.DumpInfo) (timelinesCount int64, podsCount int64, heapDumpsCount int64, tdTopDumpsCount int64, err error) {
	log.Info(ctx, "Store dumps for hour %v in cache db", tHour)
	if len(tdTopDumps) == 0 && len(heapDumps) == 0 {
		log.Info(ctx, "No dumps to store for minute %v", tHour)
		return
	}

	result, err := t.dbClient.StoreDumpsTransactionally(ctx, heapDumps, tdTopDumps, tHour)

	if err != nil {
		log.Error(ctx, err, "failed to insert dumps")
		return
	}

	timelinesCount = result.TimelinesCreated
	podsCount = result.PodsCreated
	heapDumpsCount = result.HeapDumpsInserted
	tdTopDumpsCount = result.TdTopDumpsInserted

	log.Info(ctx, "Inserted timeline: %d, pods: %d, heap dumps: %d, td/top dumps: %d",
		timelinesCount, podsCount, heapDumpsCount, tdTopDumpsCount,
	)
	if err != nil {
		log.Error(ctx, err, "failed to insert dumps for minute %v", tHour)
	}

	err = t.dbClient.Transaction(ctx, func(tx db.DumpDbClient) error {

		// Get timeline
		timeline, err := tx.FindTimeline(ctx, tHour)
		if err != nil {
			return err
		}

		log.Info(ctx, "Timeline is returned %v", timeline.TsHour)

		if timeline.Status != status {
			timeline, err = tx.UpdateTimelineStatus(ctx, tHour, status)
			if err != nil {
				return err
			}
			log.Info(ctx, "Timeline %v status is updated to %s", timeline.TsHour, status)
		}
		return nil
	})
	return
}
