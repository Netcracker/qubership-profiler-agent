package s3

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A process that scrapes the default registry sees a zero series for every
// operation type as soon as a client exists, not only after the first request.
func TestNewReadOnlyClientExposesZeroSeriesOnDefaultRegistry(t *testing.T) {
	_, err := NewReadOnlyClient(context.Background(), Params{
		Endpoint:        "127.0.0.1:9",
		AccessKeyID:     "key",
		SecretAccessKey: "secret",
		BucketName:      "profiler",
	})
	require.NoError(t, err)

	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	var operations []string
	for _, f := range families {
		if f.GetName() != "cdt_minio_operation_errors_count" {
			continue
		}
		for _, m := range f.GetMetric() {
			operations = append(operations, m.GetLabel()[0].GetValue())
		}
	}
	assert.ElementsMatch(t, []string{"get", "list", "put", "remove", "remove_many"}, operations,
		"operation_type values of cdt_minio_operation_errors_count on the default registry")
}

// A registry that already holds a different collector under a cdt_minio_* name
// is a real conflict, not a repeated registration, and must not be ignored.
func TestRegisterMetricsPanicsOnConflictingCollector(t *testing.T) {
	reg := prometheus.NewRegistry()
	reg.MustRegister(prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cdt_minio_operation_errors_count",
		Help: "a different collector under the same name",
	}))

	assert.Panics(t, func() { RegisterMetrics(reg) })
}
