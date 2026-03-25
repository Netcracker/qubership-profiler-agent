package metrics

import "github.com/prometheus/client_golang/prometheus"

type EntityLabelType string

const (
	// result label for cloud_profiler_dumps_collector-go metrics: success or fail
	resultLabelName = "result"
	resultSuccess   = "success"
	resultFail      = "fail"

	// entity label for cloud_profiler_dumps_collector-go metrics: pod, timeline, heap-dump or td-top-dump
	entityLabelName = "entity"
	EntityPod       = EntityLabelType("pod")
	EntityTimelime  = EntityLabelType("timeline")
	EntityHeapDump  = EntityLabelType("heap-dump")
	EntityTdTopDump = EntityLabelType("td-top-dump")
	NoEntity        = EntityLabelType("")
)

func init() {
	// PG metrics
	prometheus.MustRegister(pgOperationSeconds)
	prometheus.MustRegister(pgOperationAffectedEntitiesCount)

	// Task metrics
	prometheus.MustRegister(taskOperationSeconds)
	prometheus.MustRegister(taskEntitesCount)

	// Load metrics
	prometheus.MustRegister(affectedEntitiesCount)

	// Request metrics
	prometheus.MustRegister(statisticTime)
	prometheus.MustRegister(statisticTimelinesCount)
	prometheus.MustRegister(statisticPodsCount)

	prometheus.MustRegister(downloadDumpsTime)
	prometheus.MustRegister(downloadTimelinesCount)
	prometheus.MustRegister(downloadPodsCount)
	prometheus.MustRegister(downloadDumpsCount)
}

func resultLabel(isError bool) string {
	if isError {
		return resultFail
	}
	return resultSuccess
}
