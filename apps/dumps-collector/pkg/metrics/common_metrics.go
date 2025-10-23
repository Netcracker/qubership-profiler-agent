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
	prometheus.Register(pgOperationSeconds)
	prometheus.Register(pgOperationAffectedEntitiesCount)

	// Task metrics
	prometheus.Register(taskOperationSeconds)
	prometheus.Register(taskEntitesCount)

	// Load metrics
	prometheus.Register(affectedEntitiesCount)

	// Request metrics
	prometheus.Register(statisticTime)
	prometheus.Register(statisticTimelinesCount)
	prometheus.Register(statisticPodsCount)

	prometheus.Register(downloadDumpsTime)
	prometheus.Register(downloadTimelinesCount)
	prometheus.Register(downloadPodsCount)
	prometheus.Register(downloadDumpsCount)
}

func resultLabel(isError bool) string {
	if isError {
		return resultFail
	}
	return resultSuccess
}
