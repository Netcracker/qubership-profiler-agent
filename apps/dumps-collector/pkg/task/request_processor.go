package task

import (
	"archive/zip"
	"context"
	"fmt"
	"path/filepath"
	"time"

	db "github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/client"
	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/metrics"
	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/model"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"

	"github.com/google/uuid"
)

const downloadURI = "/esc/download"

type FilesLocation struct {
	PathToFiles []string
	PathToZip   string
}

type RequestProcessor struct {
	*task
	isEmulator bool
}

func NewRequestProcessor(baseDir string, dbClient db.DumpDbClient, isEmulator bool) (*RequestProcessor, error) {
	task, err := newTask(baseDir, dbClient)
	if err != nil {
		return nil, err
	}
	metrics.AddStaticticMetricValue(0, 0, 0, true)
	metrics.AddStaticticMetricValue(0, 0, 0, false)

	metrics.AddDownloadTdTopDumpsMetricValue(model.TdDumpType, 0, 0, 0, 0, true)
	metrics.AddDownloadTdTopDumpsMetricValue(model.TdDumpType, 0, 0, 0, 0, false)
	metrics.AddDownloadTdTopDumpsMetricValue(model.TopDumpType, 0, 0, 0, 0, true)
	metrics.AddDownloadTdTopDumpsMetricValue(model.TopDumpType, 0, 0, 0, 0, false)

	metrics.AddDownloadHeapDumpsMetricValue(0, true)
	metrics.AddDownloadHeapDumpsMetricValue(0, false)
	return &RequestProcessor{task: task, isEmulator: isEmulator}, nil
}

func (p *RequestProcessor) StatisticRequest(ctx context.Context, dateFrom time.Time, dateTo time.Time, podFilter model.PodFilter) ([]*model.StatisticItem, error) {
	startTime := time.Now()
	log.Info(ctx, "Statistic request received: dateFrom %v, dateTo %v", dateFrom, dateTo)

	// Fetch information about all pods matching the given filter
	pods, err := p.dbClient.SearchPods(ctx, podFilter)
	if err != nil {
		log.Error(ctx, err, "Error searching pods")
		duration := time.Since(startTime)
		metrics.AddStaticticMetricValue(duration, 0, 0, true)
		return []*model.StatisticItem{}, err
	}

	podIds := make([]uuid.UUID, len(pods))
	resultsPerPod := make(map[uuid.UUID]*model.StatisticItem, len(podIds))
	for i, pod := range pods {
		podIds[i] = pod.Id
	}

	// Search timelines in the given range (-db.Granularity + 1 is needed to handle the first timeline)
	timelines, err := p.dbClient.SearchTimelines(ctx, dateFrom.Add(-db.Granularity+1), dateTo)
	if err != nil {
		log.Error(ctx, err, "Error searching timelines")
		duration := time.Since(startTime)
		metrics.AddStaticticMetricValue(duration, int64(len(pods)), 0, true)
		return []*model.StatisticItem{}, err
	}
	log.Debug(ctx, "[StatisticRequest] found %v timelines: %s", len(timelines), timelines)

	// Calculate td/top dumps statistic per timeline
	for _, timeline := range timelines {
		if timeline.Status == model.RemovingStatus {
			log.Debug(ctx, "Timeline %v has removing status, skip it", timeline.TsHour)
		}

		// Calculate top/thread dump stats for every pod in the current timeline
		statistics, err := p.dbClient.CalculateSummaryTdTopDumps(ctx, timeline.TsHour, podIds, dateFrom, dateTo)
		if err != nil {
			log.Error(ctx, err, "Error calculating td/top dumps statistic for hour %v", timeline.TsHour)
			duration := time.Since(startTime)
			metrics.AddStaticticMetricValue(duration, int64(len(pods)), int64(len(timelines)), true)
			return []*model.StatisticItem{}, err
		}

		for _, statistic := range statistics {
			if statisticItem, found := resultsPerPod[statistic.PodId]; found {
				statisticItem.FirstSamleMillis = min(statisticItem.FirstSamleMillis, statistic.DateFrom.UnixMilli())
				statisticItem.LastSampleMillis = max(statisticItem.LastSampleMillis, statistic.DateTo.UnixMilli())
				statisticItem.DataAtEnd += statistic.SumFileSize
			} else {
				resultsPerPod[statistic.PodId] = &model.StatisticItem{
					FirstSamleMillis: statistic.DateFrom.UnixMilli(),
					LastSampleMillis: statistic.DateTo.UnixMilli(),
					DataAtStart:      0,
					DataAtEnd:        statistic.SumFileSize,
					HeapDumps:        make([]model.HeapDumpsStatistic, 0),
				}
			}
		}
	}

	// Search heap dumps
	heapDumps, err := p.dbClient.SearchHeapDumps(ctx, podIds, dateFrom, dateTo)
	if err != nil {
		log.Error(ctx, err, "Error searching heap dumps")
		duration := time.Since(startTime)
		metrics.AddStaticticMetricValue(duration, int64(len(pods)), int64(len(timelines)), true)
		return []*model.StatisticItem{}, err
	}
	for _, heapDump := range heapDumps {
		if statisticItem, found := resultsPerPod[heapDump.PodId]; found {
			statisticItem.HeapDumps = append(statisticItem.HeapDumps, model.HeapDumpsStatistic{
				Date:   heapDump.CreationTime.UnixMilli(),
				Bytes:  heapDump.FileSize,
				Handle: heapDump.Handle,
			})
		}
	}

	// Apply pod and common info
	results := make([]*model.StatisticItem, 0, len(resultsPerPod))
	for _, pod := range pods {
		if statisticItem, found := resultsPerPod[pod.Id]; found {
			statisticItem.Namespace = pod.Namespace
			statisticItem.ServiceName = pod.ServiceName
			statisticItem.PodName = pod.PodName
			statisticItem.ActiveSinceMillis = pod.RestartTime.UnixMilli()
			statisticItem.OnlineNow = pod.IsOnline()
			statisticItem.CurrentBitrate = float64(statisticItem.DataAtEnd) /
				float64((statisticItem.LastSampleMillis-statisticItem.FirstSamleMillis+1)*1000)
			statisticItem.DownloadOptions = append(statisticItem.DownloadOptions,
				model.StatisticDownloadOption{TypeName: model.TdDumpType, Uri: downloadURI},
				model.StatisticDownloadOption{TypeName: model.TopDumpType, Uri: downloadURI},
			)
			results = append(results, statisticItem)
		}
	}

	duration := time.Since(startTime)
	metrics.AddStaticticMetricValue(duration, int64(len(pods)), int64(len(timelines)), false)

	log.Info(ctx, "Statistic request finished: dateFrom %v, dateTo %v. Found %d statistic items", dateFrom, dateTo, len(results))
	return results, nil
}

func (p *RequestProcessor) TdTopDumpDownloadFiles(ctx context.Context, dateFrom time.Time, dateTo time.Time, namespace string, serviceName string, podName string, dumpType model.DumpType) ([]FilesLocation, error) {
	startTime := time.Now()
	log.Info(ctx, "Download %s file request received: dateFrom %v, dateTo %v", dumpType, dateFrom, dateTo)

	//if p.isEmulator {
	//	return []FilesLocation{{PathToFiles: []string{p.dbClient.GetParams().DbPath}}}, nil
	//}

	internalFilters := []model.PodFilter{
		model.NewPodFilterComparator("namespace", model.ComparatorEqual, namespace),
	}
	if serviceName != "" {
		internalFilters = append(internalFilters, model.NewPodFilterComparator("service_name", model.ComparatorEqual, serviceName))
	}
	if podName != "" {
		internalFilters = append(internalFilters, model.NewPodFilterComparator("pod_name", model.ComparatorEqual, podName))
	}
	podFilter := model.NewPodFilterСondition(model.OperationAnd, internalFilters...)

	pods, err := p.dbClient.SearchPods(ctx, podFilter)
	if err != nil {
		log.Error(ctx, err, "Error searching pods with namespace %s, service-name %s, pod %s", namespace, serviceName, podName)
		duration := time.Since(startTime)
		metrics.AddDownloadTdTopDumpsMetricValue(dumpType, duration, 0, 0, 0, true)
		return nil, err
	}
	if len(pods) == 0 {
		duration := time.Since(startTime)
		metrics.AddDownloadTdTopDumpsMetricValue(dumpType, duration, 0, 0, 0, false)
		return []FilesLocation{}, nil
	}
	podPerId := map[uuid.UUID]model.Pod{}
	podIds := make([]uuid.UUID, len(pods))
	for i, pod := range pods {
		podPerId[pod.Id] = pod
		podIds[i] = pod.Id
	}

	timelines, err := p.dbClient.SearchTimelines(ctx, dateFrom.Add(-db.Granularity+1), dateTo)
	if err != nil {
		log.Error(ctx, err, "Error searching  timelines from %v to %v", dateFrom, dateTo)
		duration := time.Since(startTime)
		metrics.AddDownloadTdTopDumpsMetricValue(dumpType, duration, 0, int64(len(pods)), 0, true)
		return nil, err
	}

	if len(timelines) == 0 {
		duration := time.Since(startTime)
		metrics.AddDownloadTdTopDumpsMetricValue(dumpType, duration, 0, int64(len(pods)), 0, false)
		return []FilesLocation{}, nil
	}

	dumpsCount := int64(0)
	result := make([]FilesLocation, 0, len(timelines))
	for _, timeline := range timelines {
		// Returns file paths for the current timeline. A single query will return a maximum of 60 entries
		filesLocation, err := p.collectTdTopDumpsForTimeline(ctx, namespace, podIds, podPerId, timeline, dumpType, dateFrom, dateTo)
		if err == nil {
			result = append(result, *filesLocation)
			dumpsCount += int64(len(filesLocation.PathToFiles))
		}
	}

	duration := time.Since(startTime)
	metrics.AddDownloadTdTopDumpsMetricValue(dumpType, duration, 0, int64(len(pods)), dumpsCount, false)

	log.Info(ctx, "Download %s dumps request finished: dateFrom %v, dateTo %v. Collected %d locations", dumpType, dateFrom, dateTo, len(result))
	return result, nil
}

func (p *RequestProcessor) collectTdTopDumpsForTimeline(ctx context.Context, namespace string, podIds []uuid.UUID, podPerId map[uuid.UUID]model.Pod, timeline model.Timeline,
	dumpType model.DumpType, dateFrom time.Time, dateTo time.Time) (*FilesLocation, error) {
	log.Info(ctx, "Collect %s dumps in timeline %v with time range from %v to %v", dumpType, timeline.TsHour, dateFrom, dateTo)

	dumps, err := p.dbClient.SearchTdTopDumps(ctx, timeline.TsHour, podIds, dateFrom, dateTo, dumpType)
	if err != nil {
		log.Error(ctx, err, "Error searching %s dumps from timeline %v with time range from %v to %v",
			dumpType, timeline.TsHour, dateFrom, dateTo)
		return nil, err
	}

	result := FilesLocation{PathToFiles: make([]string, 0, len(dumps))}

	if timeline.Status == model.ZippedStatus {
		result.PathToZip = filepath.Join(p.baseDir, namespace, FileHourZipInPV(timeline.TsHour))
		zipReader, err := zip.OpenReader(result.PathToZip)
		if err != nil {
			log.Error(ctx, err, "Error opening zip %s to collect dumps", result.PathToZip)
			return nil, err
		}
		defer zipReader.Close()

		filePatterns := make([]string, 0, len(dumps))
		for _, dump := range dumps {
			pod := podPerId[dump.PodId]
			filePatterns = append(filePatterns, filepath.Join(pod.Namespace,
				FileSecondDirInPV(dump.CreationTime), pod.PodName, fmt.Sprintf("*%s", dumpType.GetFileSuffix())))
		}

		for _, zipFile := range zipReader.File {
			matched := false
			for _, pattern := range filePatterns {
				matched, err = filepath.Match(pattern, zipFile.Name)
				if err != nil {
					log.Error(ctx, err, "Error checking, if file %s is matched pattern %s", zipFile.Name, pattern)
				}
				if matched {
					break
				}
			}
			if matched {
				result.PathToFiles = append(result.PathToFiles, zipFile.Name)
			}
		}

	} else {
		for _, dump := range dumps {
			pod := podPerId[dump.PodId]
			dumpFiles, err := p.getDumpsLocation(ctx, pod, dump.CreationTime, dumpType)
			if err == nil {
				result.PathToFiles = append(result.PathToFiles, dumpFiles...)
			}
		}
	}

	log.Info(ctx, "Collect %s dumps in timeline %v with time range from %v to %v finished. Collected %d files",
		dumpType, timeline.TsHour, dateFrom, dateTo, len(result.PathToFiles))
	return &result, nil
}

func (p *RequestProcessor) getDumpsLocation(ctx context.Context, pod model.Pod, creationTime time.Time, dumpType model.DumpType) ([]string, error) {
	targetDir := filepath.Join(p.baseDir, pod.Namespace, FileSecondDirInPV(creationTime), pod.PodName)
	pattern := filepath.Join(targetDir, fmt.Sprintf("*%s", dumpType.GetFileSuffix()))
	dumpFiles, err := filepath.Glob(pattern)
	if err != nil {
		log.Error(ctx, err, "Error getting %s dumps from directory %s", dumpType, targetDir)
		return nil, err
	}
	return dumpFiles, nil
}

func (p *RequestProcessor) HeapDumpDownloadFile(ctx context.Context, handle string) (*FilesLocation, error) {
	startTime := time.Now()
	log.Info(ctx, "Download heap dump request received: handle %s", handle)

	//if p.isEmulator {
	//	return &FilesLocation{PathToFiles: []string{p.dbClient.GetParams().DbPath}}, nil
	//}

	heapDump, err := p.dbClient.FindHeapDump(ctx, handle)
	if err != nil {
		log.Error(ctx, err, "Error finding heap dump with handle %s", handle)
		duration := time.Since(startTime)
		metrics.AddDownloadHeapDumpsMetricValue(duration, true)
		return nil, err
	}

	timeline, err := p.dbClient.FindTimeline(ctx, heapDump.CreationTime)
	if err != nil {
		log.Error(ctx, err, "Error finding timeline for heap dump with handle %s", handle)
		duration := time.Since(startTime)
		metrics.AddDownloadHeapDumpsMetricValue(duration, true)
		return nil, err
	}
	if timeline.Status == model.RemovingStatus {
		log.Error(ctx, nil, "Timeline %v has removing status, not possible to get heap dump with handle %s", timeline.TsHour, handle)
		duration := time.Since(startTime)
		metrics.AddDownloadHeapDumpsMetricValue(duration, true)
		return nil, fmt.Errorf("dump not found in PV")
	}

	pod, err := p.dbClient.GetPodById(ctx, heapDump.PodId)
	if err != nil {
		log.Error(ctx, err, "Error getting pod for heap dump with handle %s", handle)
		duration := time.Since(startTime)
		metrics.AddDownloadHeapDumpsMetricValue(duration, true)
		return nil, err
	}

	result := FilesLocation{}
	heapDumps, err := p.getDumpsLocation(ctx, *pod, heapDump.CreationTime, model.HeapDumpType)
	if err != nil {
		log.Error(ctx, err, "Error getting heap dump with handle %s from PV, try to find it in hour archive", handle)
		duration := time.Since(startTime)
		metrics.AddDownloadHeapDumpsMetricValue(duration, true)
		return nil, err
	}
	if len(heapDumps) == 0 {
		// TODO: find in hour archive is needed for backward compatibility only. Should be removed in  next release
		result.PathToZip = filepath.Join(p.baseDir, pod.Namespace, FileHourZipInPV(timeline.TsHour))
		zipReader, err := zip.OpenReader(result.PathToZip)
		if err != nil {
			log.Error(ctx, err, "Error opening zip %s to collect heap dumps", result.PathToZip)
			duration := time.Since(startTime)
			metrics.AddDownloadHeapDumpsMetricValue(duration, true)
			return nil, err
		}
		defer zipReader.Close()

		filePattern := filepath.Join(pod.Namespace, FileSecondDirInPV(heapDump.CreationTime),
			pod.PodName, fmt.Sprintf("*%s", model.HeapDumpType.GetFileSuffix()))

		for _, zipFile := range zipReader.File {
			matched, err := filepath.Match(filePattern, zipFile.Name)
			if err != nil {
				duration := time.Since(startTime)
				metrics.AddDownloadHeapDumpsMetricValue(duration, true)
				log.Error(ctx, err, "Error checking, if file %s is matched pattern %s", zipFile.Name, filePattern)
			}
			if matched {
				heapDumps = append(heapDumps, zipFile.Name)
				break
			}
		}

		if len(heapDumps) == 0 {
			log.Error(ctx, nil, "Heap dump with handle %s not found in PV", handle)
			duration := time.Since(startTime)
			metrics.AddDownloadHeapDumpsMetricValue(duration, true)
			return nil, fmt.Errorf("heap dump not found in PV")
		}
	}

	log.Info(ctx, "Download heap dump request finished: handle %s. Heap dump location: %s", handle, heapDumps[0])
	result.PathToFiles = heapDumps

	duration := time.Since(startTime)
	metrics.AddDownloadHeapDumpsMetricValue(duration, false)
	return &result, nil
}
