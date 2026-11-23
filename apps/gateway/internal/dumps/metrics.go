package dumps

import (
	"context"
	"errors"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/Netcracker/qubership-profiler-agent/apps/gateway/internal/telemetry"
)

// Upload outcomes, the `outcome` attribute of profiler_gateway_upload_bytes_total.
const (
	outcomeForwarded        = "forwarded"
	outcomeDiscarded        = "discarded"
	outcomeOverCap          = "over_cap"
	outcomeOverConcurrency  = "over_concurrency"
	outcomeUpstreamError    = "upstream_error"
	outcomeClientAborted    = "client_aborted"
	outcomeUpstreamRejected = "upstream_rejected"
)

// The names are the ones in plan §12. The Collector's Prometheus endpoint
// adds no type or unit suffix, so counters carry `_total` themselves and
// the instruments set no unit.
type metrics struct {
	*telemetry.Shared
	inFlight    metric.Int64UpDownCounter
	uploadBytes metric.Int64Counter
}

func newMetrics(m metric.Meter) (*metrics, error) {
	var errs [3]error
	out := &metrics{}
	out.Shared, errs[0] = telemetry.NewShared(m, "dumps")
	out.inFlight, errs[1] = m.Int64UpDownCounter("profiler_gateway_uploads_in_flight",
		metric.WithDescription("Dump uploads being forwarded or discarded now."))
	out.uploadBytes, errs[2] = m.Int64Counter("profiler_gateway_upload_bytes_total",
		metric.WithDescription("Bytes of dump uploads received, by dump type and outcome."))
	return out, errors.Join(errs[:]...)
}

func (m *metrics) upload(ctx context.Context, dumpType, outcome string, n int64) {
	m.uploadBytes.Add(ctx, n, metric.WithAttributes(
		attribute.String("dump_type", dumpType), attribute.String("outcome", outcome)))
	switch outcome {
	case outcomeForwarded:
		m.Forwarded(ctx, n)
	case outcomeDiscarded, outcomeOverCap, outcomeOverConcurrency:
		m.Discarded(ctx, n)
	}
}
