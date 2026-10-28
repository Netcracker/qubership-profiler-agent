package tcp

import (
	"context"
	"errors"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var surfaceAttr = attribute.String("surface", "tcp")

// The names are the ones in plan §12. The Collector's Prometheus endpoint
// adds no type or unit suffix, so counters carry `_total` themselves.
type metrics struct {
	active         metric.Int64UpDownCounter
	total          metric.Int64Counter
	refused        metric.Int64Counter
	closedByReload metric.Int64Counter
	bytesForwarded metric.Int64Counter
	bytesDiscarded metric.Int64Counter
	upstreamErrors metric.Int64Counter
	rulesReloads   metric.Int64Counter
}

func newMetrics(m metric.Meter) (*metrics, error) {
	var errs [8]error
	out := &metrics{}
	out.active, errs[0] = m.Int64UpDownCounter("profiler_gateway_connections_active",
		metric.WithDescription("Live agent connections, by mode."))
	out.total, errs[1] = m.Int64Counter("profiler_gateway_connections_total",
		metric.WithDescription("Accepted agent connections, by mode."))
	out.refused, errs[2] = m.Int64Counter("profiler_gateway_connections_refused_total",
		metric.WithDescription("Agent connections closed on arrival because max_connections was reached."))
	out.closedByReload, errs[3] = m.Int64Counter("profiler_gateway_connections_closed_by_reload_total",
		metric.WithDescription("Agent connections closed because a rules change altered their mode."))
	out.bytesForwarded, errs[4] = m.Int64Counter("profiler_gateway_bytes_forwarded_total",
		metric.WithDescription("Bytes relayed from agents to the upstream."))
	out.bytesDiscarded, errs[5] = m.Int64Counter("profiler_gateway_bytes_discarded_total",
		metric.WithDescription("Bytes accepted and thrown away. A non-zero rate means a discard switch is on."))
	out.upstreamErrors, errs[6] = m.Int64Counter("profiler_gateway_upstream_errors_total",
		metric.WithDescription("Upstream dial failures."))
	out.rulesReloads, errs[7] = m.Int64Counter("profiler_gateway_rules_reload_total",
		metric.WithDescription("Rules file loads, by result."))
	return out, errors.Join(errs[:]...)
}

func modeAttr(mode string) metric.MeasurementOption {
	return metric.WithAttributes(attribute.String("mode", mode))
}

// countingWriter adds every write to a byte counter, so the rate is visible
// while a connection that lives as long as its pod is still open.
type countingWriter struct {
	w       interface{ Write([]byte) (int, error) }
	counter metric.Int64Counter
}

func (c countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	if n > 0 {
		c.counter.Add(context.Background(), int64(n), metric.WithAttributes(surfaceAttr))
	}
	return n, err
}

// RecordRulesReload counts a rules file load.
func (p *Proxy) RecordRulesReload(ok bool) {
	result := "success"
	if !ok {
		result = "failure"
	}
	p.metrics.rulesReloads.Add(context.Background(), 1,
		metric.WithAttributes(surfaceAttr, attribute.String("result", result)))
}
