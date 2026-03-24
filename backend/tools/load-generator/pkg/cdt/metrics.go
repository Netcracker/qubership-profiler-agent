package cdt

import (
	"errors"

	"go.k6.io/k6/js/modules"
	"go.k6.io/k6/metrics"
)

type Metrics struct {
	dataSent     *metrics.Metric
	dataReceived *metrics.Metric

	sendMessage       *metrics.Metric
	sendMessageTiming *metrics.Metric
	sendMessageErrors *metrics.Metric

	readMessage       *metrics.Metric
	readMessageTiming *metrics.Metric
	readMessageErrors *metrics.Metric
}

func RegisterMetrics(vu modules.VU) (Metrics, error) {
	var err error
	var sm Metrics
	registry := vu.InitEnv().Registry

	if sm.dataSent, err = registry.NewMetric(metrics.DataSentName, metrics.Counter, metrics.Data); err != nil {
		return sm, errors.Unwrap(err)
	}

	if sm.dataReceived, err = registry.NewMetric(metrics.DataReceivedName, metrics.Counter, metrics.Data); err != nil {
		return sm, errors.Unwrap(err)
	}

	if sm.sendMessage, err = registry.NewMetric("cdt_send_count", metrics.Counter); err != nil {
		return sm, errors.Unwrap(err)
	}

	if sm.sendMessageTiming, err = registry.NewMetric("cdt_send_time", metrics.Trend, metrics.Time); err != nil {
		return sm, errors.Unwrap(err)
	}

	if sm.sendMessageErrors, err = registry.NewMetric("cdt_send_error_count", metrics.Counter); err != nil {
		return sm, errors.Unwrap(err)
	}

	if sm.readMessage, err = registry.NewMetric("cdt_read_count", metrics.Counter); err != nil {
		return sm, errors.Unwrap(err)
	}

	if sm.readMessageTiming, err = registry.NewMetric("cdt_read_time", metrics.Trend, metrics.Time); err != nil {
		return sm, errors.Unwrap(err)
	}

	if sm.readMessageErrors, err = registry.NewMetric("cdt_read_error_count", metrics.Counter); err != nil {
		return sm, errors.Unwrap(err)
	}

	return sm, nil
}
