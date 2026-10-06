package task

import (
	"context"
	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/envconfig"
	"os"
	"path/filepath"
	"strings"
	"time"

	db "github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/client"
	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/metrics"
	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/model"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
)

type InsertTask struct {
	*task
}

func NewInsertTask(baseDir string, dbClient db.DumpDbClient) (*InsertTask, error) {
	task, err := newTask(baseDir, dbClient)
	if err != nil {
		return nil, err
	}
	metrics.AddTaskMetricValue(metrics.EntityTimelime, metrics.TaskInsert, 0, 0, false)
	metrics.AddTaskMetricValue(metrics.EntityPod, metrics.TaskInsert, 0, 0, false)
	metrics.AddTaskMetricValue(metrics.EntityHeapDump, metrics.TaskInsert, 0, 0, false)
	metrics.AddTaskMetricValue(metrics.EntityTdTopDump, metrics.TaskInsert, 0, 0, false)

	metrics.AddTaskMetricValue(metrics.EntityTimelime, metrics.TaskInsert, 0, 0, true)
	metrics.AddTaskMetricValue(metrics.EntityPod, metrics.TaskInsert, 0, 0, true)
	metrics.AddTaskMetricValue(metrics.EntityHeapDump, metrics.TaskInsert, 0, 0, true)
	metrics.AddTaskMetricValue(metrics.EntityTdTopDump, metrics.TaskInsert, 0, 0, true)
	return &InsertTask{task: task}, nil
}

func (t *InsertTask) Execute(ctx context.Context, from time.Time, to time.Time) error {
	log.Info(ctx, "Execute insert operation for time range from %v to %v", from, to)

	hasHeapDumps := false

	// Iterate over each minute between 'from' (inclusive) and 'to' (exclusive)
	for tMinute := from.Truncate(time.Minute); tMinute.Before(to); tMinute = tMinute.Add(time.Minute) {
		startTime := time.Now()

		// Collect dump info for the current minute and store its metadata in the database.
		tdTopDumps, heapDumps := t.collectDumpsForMinute(ctx, tMinute)
		timelinesCount, podsCount, heapDumpsCount, tdTopDumpsCount, err := t.storeDumps(ctx, tMinute, tdTopDumps, heapDumps)

		if heapDumpsCount > 0 {
			hasHeapDumps = true
		}

		duration := time.Since(startTime)
		metrics.AddTaskMetricValue(metrics.EntityTimelime, metrics.TaskInsert, duration, timelinesCount, err != nil)
		metrics.AddTaskMetricValue(metrics.EntityPod, metrics.TaskInsert, duration, podsCount, err != nil)
		metrics.AddTaskMetricValue(metrics.EntityHeapDump, metrics.TaskInsert, duration, heapDumpsCount, err != nil)
		metrics.AddTaskMetricValue(metrics.EntityTdTopDump, metrics.TaskInsert, duration, tdTopDumpsCount, err != nil)

		if err == nil {
			metrics.AddActiveEntitiesMetricValue(metrics.EntityTimelime, timelinesCount)
			metrics.AddActiveEntitiesMetricValue(metrics.EntityPod, podsCount)
			metrics.AddActiveEntitiesMetricValue(metrics.EntityHeapDump, heapDumpsCount)
			metrics.AddActiveEntitiesMetricValue(metrics.EntityTdTopDump, tdTopDumpsCount)
		}
	}

	// Deletes old heap dumps if their number is greater than DIAG_PV_MAX_HEAP_DUMPS_PER_POD.
	// It is executed only if at least one heap dump was added during the Insert Task process
	if hasHeapDumps {
		t.trimHeapDumps(ctx)
	}

	log.Info(ctx, "Insert operation for time range from %v to %v is finished", from, to)
	return nil
}

// trimHeapDumps removes heap dump files that exceed the configured limit per pod.
// It runs a DB-side cleanup via the PG function trim_heap_dumps, then deletes matching
// files from the persistent volume based on timestamp and pod name.
func (t *InsertTask) trimHeapDumps(ctx context.Context) {
	err := t.dbClient.Transaction(ctx, func(tx db.DumpDbClient) error {
		// Calls the PG function that deletes heap dumps exceeding the limit and returns removed entries
		removedHeapDumps, _ := tx.TrimHeapDumps(ctx, envconfig.EnvConfig.MaxHeapDumps)

		for _, dump := range removedHeapDumps {
			log.Info(ctx, "Process heap dump: %+v", dump)

			// Extract pod name from the handle
			podName := strings.Split(dump.Handle, "-heap-")[0]

			// Build a glob pattern to find the corresponding .hprof.zip file on disk
			pattern := filepath.Join(
				t.baseDir,
				"**",
				FileSecondDirInPV(dump.CreationTime.Truncate(time.Second)),
				podName,
				"*.hprof.zip",
			)

			// Find matching files
			files, err := filepath.Glob(pattern)
			if err != nil {
				return err
			}

			if len(files) == 0 {
				// No file found for this dump
				log.Warning(ctx, "No files matched pattern for dump %+v", dump)
			} else {
				// Exactly one file found — remove it
				if err := os.Remove(files[0]); err != nil {
					return err
				}
				log.Info(ctx, "Successfully removed heap dump from PV: %+v", dump)
			}
		}

		log.Info(ctx, "Total removed dumps: %v", len(removedHeapDumps))
		return nil
	})

	if err != nil {
		log.Error(ctx, err, "Failed to trim heap dumps")
	}
}

// collectDumpsForMinute collects heap and thread/top dump information from PV for the given minute.
//
// For example:
//
//	tMinute: 2024-07-31 23:56:00 +0000
//
// Returns:
//
//   - tdTopDumpsInfo:  slice of DumpInfo for thread and top dumps.
//   - heapDumpsInfo:   slice of DumpInfo for heap dumps.
func (t *InsertTask) collectDumpsForMinute(ctx context.Context, tMinute time.Time) ([]model.DumpInfo, []model.DumpInfo) {
	log.Info(ctx, "Collect dumps for minute %v", tMinute)
	// e.g., "output/**/2024/07/31/23/56/**/**/*.*"
	pattern := filepath.Join(t.baseDir, "**", FileMinuteDirInPV(tMinute), "**", "**", "*.*")

	tdTopDumpsInfo, heapDumpsInfo := t.collectDumpsFromPattern(ctx, pattern)
	log.Info(ctx, "Successfuly parsed %d td/top dumps and %d heap dumps for minute %v", len(tdTopDumpsInfo), len(heapDumpsInfo), tMinute)
	return tdTopDumpsInfo, heapDumpsInfo
}

// storeDumps saves metadata about dumps to the database.
func (t *InsertTask) storeDumps(ctx context.Context, tMinute time.Time, tdTopDumps []model.DumpInfo, heapDumps []model.DumpInfo) (timelinesCount int64, podsCount int64, heapDumpsCount int64, tdTopDumpsCount int64, err error) {
	log.Info(ctx, "Store dumps for minute %v in cache db", tMinute)

	if len(tdTopDumps) == 0 && len(heapDumps) == 0 {
		log.Info(ctx, "No dumps to store for minute %v", tMinute)
		return
	}

	result, err := t.dbClient.StoreDumpsTransactionally(ctx, heapDumps, tdTopDumps, tMinute)

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
		log.Error(ctx, err, "failed to insert dumps for minute %v", tMinute)
	}
	return
}
