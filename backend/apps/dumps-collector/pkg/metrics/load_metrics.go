package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	// cloud_profiler_dumps_collector_active_entities_count metric
	// supported labels:
	// * "entity": "pod", "timeline", "td-top-dumps" or "heap-dumps"
	affectedEntitiesCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cloud_profiler_dumps_collector_active_entities_count",
			Help: "Current entities count in cloud-profiler-dumps-collector",
		},
		[]string{entityLabelName},
	)
)

func AddActiveEntitiesMetricValue(entity EntityLabelType, valuesCount int64) {
	affectedEntitiesCount.With(prometheus.Labels{
		entityLabelName: string(entity),
	}).Add(float64(valuesCount))
}
func RemoveActiveEntitiesMetricValue(entity EntityLabelType, valuesCount int64) {
	affectedEntitiesCount.With(prometheus.Labels{
		entityLabelName: string(entity),
	}).Add(float64(-valuesCount))
}
