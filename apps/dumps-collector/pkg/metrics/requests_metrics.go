package metrics

import (
	"time"

	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/model"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	// cloud_profiler_dumps_collector_statistic_time_seconds metric
	// supported labels:
	// * "result": "success", "fail"
	statisticTime = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "cloud_profiler_dumps_collector_statistic_time_seconds",
			Help: "Total time in seconds for statistic request",
		},
		[]string{resultLabelName},
	)

	// cloud_profiler_dumps_collector_statistic_processed_timelines_count metric
	// supported labels:
	// * "result": "success", "fail"
	statisticTimelinesCount = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "cloud_profiler_dumps_collector_statistic_processed_timelines_count",
			Help: "Total timelines count for statistic request",
		},
		[]string{resultLabelName},
	)

	// cloud_profiler_dumps_collector_statistic_processed_pods_count metric
	// supported labels:
	// * "result": "success", "fail"
	statisticPodsCount = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "cloud_profiler_dumps_collector_statistic_processed_pods_count",
			Help: "Total pods count for statistic request",
		},
		[]string{resultLabelName},
	)

	// cloud_profiler_dumps_collector_download_dumps_time_seconds metric
	// supported labels:
	// * "entity": "td", "top", "heap"
	// * "result": "success", "fail"
	downloadDumpsTime = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "cloud_profiler_dumps_collector_download_dumps_time_seconds",
			Help: "Total time in seconds for download request",
		},
		[]string{resultLabelName, entityLabelName},
	)

	// cloud_profiler_dumps_collector_download_dumps_processed_timelines_count metric
	// supported labels:
	// * "entity": "td", "top"
	// * "result": "success", "fail"
	downloadTimelinesCount = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "cloud_profiler_dumps_collector_download_dumps_processed_timelines_count",
			Help: "Total timelines count for download request",
		},
		[]string{resultLabelName, entityLabelName},
	)

	// cloud_profiler_dumps_collector_download_dumps_processed_pods_count metric
	// supported labels:
	// * "entity": "td", "top"
	// * "result": "success", "fail"
	downloadPodsCount = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "cloud_profiler_dumps_collector_download_dumps_processed_pods_count",
			Help: "Total pods count for download request",
		},
		[]string{resultLabelName, entityLabelName},
	)

	// cloud_profiler_dumps_collector_download_dumps_processed_dumps_count metric
	// supported labels:
	// * "entity": "td", "top"
	// * "result": "success", "fail"
	downloadDumpsCount = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "cloud_profiler_dumps_collector_download_dumps_processed_dumps_count",
			Help: "Total dumps count for download request",
		},
		[]string{resultLabelName, entityLabelName},
	)
)

func AddStaticticMetricValue(duration time.Duration, timelinesCount int64, podsCount int64, isError bool) {
	statisticTime.With(prometheus.Labels{
		resultLabelName: resultLabel(isError),
	}).Observe(duration.Seconds())
	statisticTimelinesCount.With(prometheus.Labels{
		resultLabelName: resultLabel(isError),
	}).Observe(float64(timelinesCount))
	statisticPodsCount.With(prometheus.Labels{
		resultLabelName: resultLabel(isError),
	}).Observe(float64(podsCount))
}

func AddDownloadTdTopDumpsMetricValue(dumpType model.DumpType, duration time.Duration, timelinesCount int64, podsCount int64, dumpsCount int64, isError bool) {
	downloadDumpsTime.With(prometheus.Labels{
		resultLabelName: resultLabel(isError),
		entityLabelName: string(dumpType),
	}).Observe(duration.Seconds())
	downloadTimelinesCount.With(prometheus.Labels{
		resultLabelName: resultLabel(isError),
		entityLabelName: string(dumpType),
	}).Observe(float64(timelinesCount))
	downloadPodsCount.With(prometheus.Labels{
		resultLabelName: resultLabel(isError),
		entityLabelName: string(dumpType),
	}).Observe(float64(podsCount))
	downloadDumpsCount.With(prometheus.Labels{
		resultLabelName: resultLabel(isError),
		entityLabelName: string(dumpType),
	}).Observe(float64(dumpsCount))
}

func AddDownloadHeapDumpsMetricValue(duration time.Duration, isError bool) {
	downloadDumpsTime.With(prometheus.Labels{
		resultLabelName: resultLabel(isError),
		entityLabelName: string(model.HeapDumpType),
	}).Observe(duration.Seconds())
}
