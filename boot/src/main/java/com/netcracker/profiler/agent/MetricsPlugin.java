package com.netcracker.profiler.agent;

import java.util.Map;

public interface MetricsPlugin {
    Metric getMetric(MetricType metricType, String callType, Map<String, String> aggregationParameters);
}
