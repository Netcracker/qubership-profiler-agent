package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type TaskLabelType string

const (
	// task label for cloud_profiler_dumps_collector-go task metrics
	taskTypeLabelName = "task"
	TaskInsert        = TaskLabelType("insert")
	TaskPack          = TaskLabelType("pack")
	TaskRemove        = TaskLabelType("remove")
	TaskRescan        = TaskLabelType("rescan")
)

var (
	// cloud_profiler_dumps_collector_task_operation_seconds metric
	// supported labels:
	// * "entity": "pod", "timeline", "td-top-dumps" or "heap-dumps"
	// * "task": "insert", "pack", "remove", "rescan"
	// * "result": "success" or "fail"
	taskOperationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "cloud_profiler_dumps_collector_task_operation_seconds",
			Help: "Processing task operation time in seconds for cloud-profiler-dumps-collector",
		},
		[]string{entityLabelName, resultLabelName, taskTypeLabelName},
	)

	// cloud_profiler_dumps_collector_task_processed_entities metric
	// supported labels:
	// * "entity": "pod", "timeline", "td-top-dumps" or "heap-dumps"
	// * "task": "insert", "pack", "remove", "rescan"
	// * "result": "success" or "fail"
	taskEntitesCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cloud_profiler_dumps_collector_task_processed_entities",
			Help: "Processing task operation processed entities for cloud-profiler-dumps-collector",
		},
		[]string{entityLabelName, resultLabelName, taskTypeLabelName},
	)
)

func AddTaskMetricValue(entity EntityLabelType, task TaskLabelType, duration time.Duration, processedEntites int64, isError bool) {
	taskOperationSeconds.With(prometheus.Labels{
		entityLabelName:   string(entity),
		taskTypeLabelName: string(task),
		resultLabelName:   resultLabel(isError),
	}).Observe(duration.Seconds())
	taskEntitesCount.With(prometheus.Labels{
		entityLabelName:   string(entity),
		taskTypeLabelName: string(task),
		resultLabelName:   resultLabel(isError),
	}).Add(float64(processedEntites))
}
