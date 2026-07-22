package cdt

import (
	"go.k6.io/k6/metrics"
)

// fleetMetrics holds the custom k6 series the fleet emits. Prometheus
// remote-write surfaces them as k6_vdumper_* (counters gain a _total suffix,
// trends expand into the configured stats), which is what
// dashboards/k6-load-generator.json queries.
type fleetMetrics struct {
	sentBytes     *metrics.Metric // per RCV_DATA payload, tagged stream
	connects      *metrics.Metric // successful handshakes + stream setups
	reconnects    *metrics.Metric // dead incarnations (the agent's restart path)
	churns        *metrics.Metric // deliberate churn-mode cycles (T5 storms)
	ackErrors     *metrics.Metric // ACK_ERROR_MAGIC refusals (backpressure)
	droppedChunks *metrics.Metric // chunks lost in reconnect drop windows

	tcpConnectTime   *metrics.Metric // dial only
	sessionReadyTime *metrics.Metric // dial start -> all seven streams open
	ackFlushTime     *metrics.Metric // per-stream sync ack drain, flush cycle only
}

func registerMetrics(reg *metrics.Registry) (fleetMetrics, error) {
	var m fleetMetrics
	var err error
	counter := func(name string, valueType metrics.ValueType) *metrics.Metric {
		if err != nil {
			return nil
		}
		var c *metrics.Metric
		c, err = reg.NewMetric(name, metrics.Counter, valueType)
		return c
	}
	trend := func(name string) *metrics.Metric {
		if err != nil {
			return nil
		}
		var tr *metrics.Metric
		tr, err = reg.NewMetric(name, metrics.Trend, metrics.Time)
		return tr
	}

	m.sentBytes = counter("vdumper_sent_bytes", metrics.Data)
	m.connects = counter("vdumper_connects", metrics.Default)
	m.reconnects = counter("vdumper_reconnects", metrics.Default)
	m.churns = counter("vdumper_churns", metrics.Default)
	m.ackErrors = counter("vdumper_ack_errors", metrics.Default)
	m.droppedChunks = counter("vdumper_dropped_chunks", metrics.Default)
	m.tcpConnectTime = trend("vdumper_tcp_connect_time")
	m.sessionReadyTime = trend("vdumper_session_ready_time")
	m.ackFlushTime = trend("vdumper_ack_flush_time")
	return m, err
}
