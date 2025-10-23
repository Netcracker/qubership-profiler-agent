package task

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	db "github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/client"
	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/metrics"
	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/model"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
)

type PackTask struct {
	*task
}

func NewPackTask(baseDir string, dbClient db.DumpDbClient) (*PackTask, error) {
	task, err := newTask(baseDir, dbClient)
	if err != nil {
		return nil, err
	}
	metrics.AddTaskMetricValue(metrics.EntityTimelime, metrics.TaskPack, 0, 0, false)
	metrics.AddTaskMetricValue(metrics.EntityTdTopDump, metrics.TaskPack, 0, 0, false)

	metrics.AddTaskMetricValue(metrics.EntityTimelime, metrics.TaskPack, 0, 0, true)
	metrics.AddTaskMetricValue(metrics.EntityTdTopDump, metrics.TaskPack, 0, 0, true)
	return &PackTask{task: task}, nil
}

func (pt *PackTask) Execute(ctx context.Context, tBefore time.Time) error {
	startTime := time.Now()

	log.Info(ctx, "Execute pack operation for time hours before %v", tBefore)
	timelines, err := pt.dbClient.SearchTimelines(ctx, time.Time{}, tBefore)
	if err != nil {
		log.Error(ctx, err, "Error getting timelines from db")
		duration := time.Since(startTime)
		metrics.AddTaskMetricValue(metrics.EntityTimelime, metrics.TaskPack, duration, 0, true)
		metrics.AddTaskMetricValue(metrics.EntityTdTopDump, metrics.TaskPack, duration, 0, true)
		return err
	}
	log.Info(ctx, "Found %d timelines to pack", len(timelines))

	// Find namespaces, registered in db
	namespaces, err := pt.collectNamespaces(ctx)
	if err != nil {
		duration := time.Since(startTime)
		metrics.AddTaskMetricValue(metrics.EntityTimelime, metrics.TaskPack, duration, int64(len(timelines)), true)
		metrics.AddTaskMetricValue(metrics.EntityTdTopDump, metrics.TaskPack, duration, 0, true)
		return err
	}

	for _, timeline := range timelines {
		if timeline.Status != model.RawStatus && timeline.Status != model.ZippingStatus {
			continue
		}
		startTime := time.Now()
		tdTopDumpsCount, err := pt.processTimeline(ctx, timeline, namespaces)
		duration := time.Since(startTime)
		metrics.AddTaskMetricValue(metrics.EntityTimelime, metrics.TaskPack, duration, 1, err != nil)
		metrics.AddTaskMetricValue(metrics.EntityTdTopDump, metrics.TaskPack, duration, tdTopDumpsCount, err != nil)
	}
	log.Info(ctx, "Pack operation for time hours before %v is finished", tBefore)
	return nil
}

func (pt *PackTask) processTimeline(ctx context.Context, timeline model.Timeline, namespaces []string) (tdTopDumpsCount int64, err error) {
	log.Info(ctx, "Execute packing operation for time hour %v", timeline.TsHour)

	// Process timeline per namespace, if it has raw or zipping status
	// Store unsucessful namespaces
	unsuccessfulNamespaces := make([]string, 0)
	log.Info(ctx, "Updating status for timeline %v to %s...", timeline.TsHour, model.ZippingStatus)
	// Update status to zipping
	if _, err = pt.dbClient.UpdateTimelineStatus(ctx, timeline.TsHour, model.ZippingStatus); err != nil {
		log.Error(ctx, err, "Error set zipping status for time hour %v", timeline.TsHour)
		return
	}
	log.Info(ctx, "Status for timeline %v updated", timeline.TsHour)

	// Pack files
	for _, namespace := range namespaces {
		nsTdTopDumpsCount, err := pt.processNamespace(ctx, timeline.TsHour, namespace)
		if err != nil {
			log.Error(ctx, err, "Error packing hour %v for namespace %s", timeline.TsHour, namespace)
			unsuccessfulNamespaces = append(unsuccessfulNamespaces, namespace)
		}
		tdTopDumpsCount += nsTdTopDumpsCount
	}

	// If all namespaces are processed successfully, update timeline status and remove files
	if len(unsuccessfulNamespaces) == 0 {
		// Update status to zipped
		if _, err := pt.dbClient.UpdateTimelineStatus(ctx, timeline.TsHour, model.ZippedStatus); err != nil {
			log.Error(ctx, err, "Error set zipped status for time hour %v", timeline.TsHour)
			return tdTopDumpsCount, err
		}

		// Remove files
		for _, namespace := range namespaces {
			if err := pt.clearNamespace(ctx, timeline.TsHour, namespace); err != nil {
				log.Error(ctx, err, "Error cleaning hour %v for namespace %s", timeline.TsHour, namespace)
				unsuccessfulNamespaces = append(unsuccessfulNamespaces, namespace)
			}
		}
	} else {
		return tdTopDumpsCount, fmt.Errorf("error processing namespaces: %v", unsuccessfulNamespaces)
	}

	log.Info(ctx, "Packing operation for time hour %v is finished", timeline.TsHour)
	return
}

func (pt *PackTask) processNamespace(ctx context.Context, tHour time.Time, namespace string) (tdTopDumpsCount int64, err error) {
	log.Info(ctx, "Start packing %v hour for namespace %s...", tHour, namespace)

	// e.g., output/test-namespace-1/2024/07/31
	dayDir := filepath.Join(pt.baseDir, namespace, FileDayDirInPV(tHour))

	// e.g., output/test-namespace-1/2024/07/31/23
	hourDir := filepath.Join(pt.baseDir, namespace, FileHourDirInPV(tHour))

	// e.g., output/test-namespace-1/2024/07/31/23.zip
	fullArchiveName := filepath.Join(dayDir, HourArchiveName(tHour))

	// Create zip archive, and return if it's already created
	zipFile, err := pt.createArchiveWithForce(fullArchiveName)
	if err != nil {
		log.Error(ctx, err, "Error creating hour archive %s", fullArchiveName)
		return 0, err
	}
	defer zipFile.Close()

	// Add files to archive
	w := zip.NewWriter(zipFile)
	defer w.Close()

	walker := func(path string, info os.FileInfo, err error) error {
		// process error to the end
		if err != nil {
			return err
		}

		// skip directories (they are processed silently)
		if info.IsDir() {
			return nil
		}

		// skip heap dumps
		if strings.HasSuffix(path, model.HeapDumpType.GetFileSuffix()) {
			return nil
		}

		tdTopDumpsCount++
		file, err := os.Open(path)
		if err != nil {
			log.Error(ctx, err, "Error opening file %s", file)
			return err
		}
		defer file.Close()

		zipPath, _ := pt.splitBaseDirFromPath(path)
		fileInZip, err := w.Create(zipPath)
		if err != nil {
			log.Error(ctx, err, "Error creating file with path %s in zip archive %s", zipPath, fullArchiveName)
			return err
		}

		_, err = io.Copy(fileInZip, file)
		if err != nil {
			log.Error(ctx, err, "Error coping file with path %s in zip archive %s", path, fullArchiveName)
			return err
		}

		return nil
	}

	if err := filepath.Walk(hourDir, walker); err != nil {
		return tdTopDumpsCount, err
	}

	log.Info(ctx, "Finished packing %v hour for namespace %s...", tHour, namespace)
	return
}

func (pt *PackTask) clearNamespace(ctx context.Context, tHour time.Time, namespace string) error {
	log.Info(ctx, "Start removing %v hour for namespace %s...", tHour, namespace)
	hourDir := filepath.Join(pt.baseDir, namespace, FileHourDirInPV(tHour))

	info, err := os.Lstat(hourDir)
	if err := pt.clearNamespaceWalker(hourDir, info, err); err != nil {
		log.Error(ctx, err, "Error clearing directory %s", hourDir)
		return err
	}

	log.Info(ctx, "Finished removing %v hour for namespace %s...", tHour, namespace)
	return nil
}

func (pt *PackTask) clearNamespaceWalker(path string, info os.FileInfo, err error) error {
	// process error to the end
	if err != nil {
		return err
	}

	// process directory
	if info.IsDir() {
		relPath, err := pt.splitBaseDirFromPath(path)
		if err != nil {
			return err
		}
		parts := splitDir(relPath)
		heapPattern := path
		for i := 0; i < 8-len(parts); i++ {
			heapPattern = filepath.Join(heapPattern, "*")
		}
		heapPattern = filepath.Join(heapPattern, fmt.Sprintf("*%s", model.HeapDumpType.GetFileSuffix()))
		heapFiles, err := filepath.Glob(heapPattern)
		if err != nil {
			return err
		}
		if len(heapFiles) == 0 {
			return os.RemoveAll(path)
		}
		internalFiles, err := os.ReadDir(path)
		if err != nil {
			return err
		}
		for _, file := range internalFiles {
			info, err = file.Info()
			err = pt.clearNamespaceWalker(filepath.Join(path, file.Name()), info, err)
		}
		return err
	}
	if strings.HasSuffix(path, model.HeapDumpType.GetFileSuffix()) {
		return nil
	}
	return os.Remove(path)
}

func (pt *PackTask) createArchiveWithForce(fullArchiveName string) (*os.File, error) {
	_, err := os.Stat(fullArchiveName)
	if os.IsNotExist(err) {
		// Create empty archive if it's not exist
		return os.Create(fullArchiveName)
	} else if err != nil {
		// Handle unexpected error
		return nil, err
	}

	// File exists, remove it and recreate
	if err := os.Remove(fullArchiveName); err != nil {
		return nil, err
	}

	return os.Create(fullArchiveName)
}
