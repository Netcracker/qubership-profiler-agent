// Package telemetry declares the metric families that both the dumps and the
// TCP surface report, so each family has one name and one description.
package telemetry

import (
	"context"
	"errors"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Shared records the families both surfaces report. Every measurement
// carries the `surface` attribute given to NewShared.
type Shared struct {
	surface        attribute.KeyValue
	bytesForwarded metric.Int64Counter
	bytesDiscarded metric.Int64Counter
	upstreamErrors metric.Int64Counter
	rulesReloads   metric.Int64Counter
	// surfaceOpt is built once: metric.WithAttributes builds and sorts an
	// attribute set on every call, and Forwarded and Discarded run per write.
	surfaceOpt metric.MeasurementOption
}

// NewShared creates the shared instruments on m for surface, "dumps" or "tcp".
// The names are the ones in the "Metrics" section of
// docs/plans/2026-10-09-otel-gateway.md. The Collector's Prometheus endpoint
// adds no type or unit suffix, so counters carry `_total` themselves.
func NewShared(m metric.Meter, surface string) (*Shared, error) {
	var errs [4]error
	s := &Shared{surface: attribute.String("surface", surface)}
	s.surfaceOpt = metric.WithAttributeSet(attribute.NewSet(s.surface))
	s.bytesForwarded, errs[0] = m.Int64Counter("profiler_gateway_bytes_forwarded_total",
		metric.WithDescription("Bytes relayed to the upstream."))
	s.bytesDiscarded, errs[1] = m.Int64Counter("profiler_gateway_bytes_discarded_total",
		metric.WithDescription("Bytes accepted and thrown away: by discard mode or a rule, or over the size or concurrency cap."))
	s.upstreamErrors, errs[2] = m.Int64Counter("profiler_gateway_upstream_errors_total",
		metric.WithDescription("Upstream dial and transport failures."))
	s.rulesReloads, errs[3] = m.Int64Counter("profiler_gateway_rules_reload_total",
		metric.WithDescription("Rules file loads, by result."))
	return s, errors.Join(errs[:]...)
}

// Forwarded counts n bytes relayed to the upstream.
func (s *Shared) Forwarded(ctx context.Context, n int64) {
	s.bytesForwarded.Add(ctx, n, s.surfaceOpt)
}

// Discarded counts n bytes accepted and thrown away.
func (s *Shared) Discarded(ctx context.Context, n int64) {
	s.bytesDiscarded.Add(ctx, n, s.surfaceOpt)
}

// UpstreamError counts one upstream dial or transport failure.
func (s *Shared) UpstreamError(ctx context.Context) {
	s.upstreamErrors.Add(ctx, 1, s.surfaceOpt)
}

// RulesReloaded counts a rules file load, with result "success" or "failure".
func (s *Shared) RulesReloaded(ok bool) {
	result := "success"
	if !ok {
		result = "failure"
	}
	s.rulesReloads.Add(context.Background(), 1, metric.WithAttributes(s.surface, attribute.String("result", result)))
}
