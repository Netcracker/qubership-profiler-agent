package tcp

import (
	"context"
	"errors"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/Netcracker/qubership-profiler-agent/apps/gateway/internal/telemetry"
)

// metrics holds the agent-connection instruments, named as in the "Metrics"
// section of docs/plans/2026-10-09-otel-gateway.md. The Collector's
// Prometheus endpoint adds no type or unit suffix, so counters carry `_total`
// themselves.
type metrics struct {
	*telemetry.Shared
	active         metric.Int64UpDownCounter
	total          metric.Int64Counter
	refused        metric.Int64Counter
	closedByReload metric.Int64Counter
}

func newMetrics(m metric.Meter) (*metrics, error) {
	var errs [5]error
	out := &metrics{}
	out.Shared, errs[0] = telemetry.NewShared(m, "tcp")
	out.active, errs[1] = m.Int64UpDownCounter("profiler_gateway_connections_active",
		metric.WithDescription("Live agent connections, by mode."))
	out.total, errs[2] = m.Int64Counter("profiler_gateway_connections_total",
		metric.WithDescription("Agent sessions started, by mode. A discard session starts after a valid handshake, "+
			"a passthrough session after the upstream dial succeeds; refused connections, bad handshakes, "+
			"and failed dials are not counted."))
	out.refused, errs[3] = m.Int64Counter("profiler_gateway_connections_refused_total",
		metric.WithDescription("Agent connections closed on arrival because max_connections was reached."))
	out.closedByReload, errs[4] = m.Int64Counter("profiler_gateway_connections_closed_by_reload_total",
		metric.WithDescription("Agent connections closed because a rules change altered their mode."))
	return out, errors.Join(errs[:]...)
}

func modeAttr(mode string) metric.MeasurementOption {
	return metric.WithAttributes(attribute.String("mode", mode))
}

// countingWriter counts every write as bytes forwarded, so the rate is
// visible while a connection that lives as long as its pod is still open.
type countingWriter struct {
	w       interface{ Write([]byte) (int, error) }
	metrics *metrics
}

func (c countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	if n > 0 {
		c.metrics.Forwarded(context.Background(), int64(n))
	}
	return n, err
}

// RecordRulesReload counts a rules file load.
func (p *Proxy) RecordRulesReload(ok bool) { p.metrics.RulesReloaded(ok) }
