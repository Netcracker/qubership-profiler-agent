package dumps

import (
	"context"
	"errors"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Upload outcomes, the `outcome` attribute of profiler_gateway_upload_bytes_total.
const (
	outcomeForwarded       = "forwarded"
	outcomeDiscarded       = "discarded"
	outcomeOverCap         = "over_cap"
	outcomeOverConcurrency = "over_concurrency"
	outcomeUpstreamError   = "upstream_error"
	outcomeClientAborted   = "client_aborted"
)

var surfaceAttr = attribute.String("surface", "dumps")

// The names are the ones in plan §12. The Collector's Prometheus endpoint
// adds no type or unit suffix, so counters carry `_total` themselves and
// the instruments set no unit.
type metrics struct {
	inFlight       metric.Int64UpDownCounter
	uploadBytes    metric.Int64Counter
	bytesForwarded metric.Int64Counter
	bytesDiscarded metric.Int64Counter
	upstreamErrors metric.Int64Counter
	rulesReloads   metric.Int64Counter
}

func newMetrics(m metric.Meter) (*metrics, error) {
	var errs [6]error
	out := &metrics{}
	out.inFlight, errs[0] = m.Int64UpDownCounter("profiler_gateway_uploads_in_flight",
		metric.WithDescription("Dump uploads being forwarded or discarded now."))
	out.uploadBytes, errs[1] = m.Int64Counter("profiler_gateway_upload_bytes_total",
		metric.WithDescription("Bytes of dump uploads received, by dump type and outcome."))
	out.bytesForwarded, errs[2] = m.Int64Counter("profiler_gateway_bytes_forwarded_total",
		metric.WithDescription("Bytes relayed to the upstream."))
	out.bytesDiscarded, errs[3] = m.Int64Counter("profiler_gateway_bytes_discarded_total",
		metric.WithDescription("Bytes accepted and thrown away. A non-zero rate means a discard switch is on."))
	out.upstreamErrors, errs[4] = m.Int64Counter("profiler_gateway_upstream_errors_total",
		metric.WithDescription("Upstream dial and transport failures."))
	out.rulesReloads, errs[5] = m.Int64Counter("profiler_gateway_rules_reload_total",
		metric.WithDescription("Rules file loads, by result."))
	return out, errors.Join(errs[:]...)
}

func (m *metrics) upload(ctx context.Context, dumpType, outcome string, n int64) {
	m.uploadBytes.Add(ctx, n, metric.WithAttributes(
		attribute.String("dump_type", dumpType), attribute.String("outcome", outcome)))
	switch outcome {
	case outcomeForwarded:
		m.bytesForwarded.Add(ctx, n, metric.WithAttributes(surfaceAttr))
	case outcomeDiscarded, outcomeOverCap, outcomeOverConcurrency:
		m.bytesDiscarded.Add(ctx, n, metric.WithAttributes(surfaceAttr))
	}
}

// rulesReloaded counts a rules file load; result is "success" or "failure".
func (m *metrics) rulesReloaded(result string) {
	m.rulesReloads.Add(context.Background(), 1, metric.WithAttributes(surfaceAttr, attribute.String("result", result)))
}
