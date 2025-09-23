package com.netcracker.profiler.metrics;

import com.netcracker.profiler.agent.MetricType;

import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class CountMetric extends AbstractMetric {

    private static final MetricType METRIC_TYPE = MetricType.COUNT;
    private static final MetricUnit METRIC_UNIT = MetricUnit.TOTAL;

    private AtomicLong count = new AtomicLong();

    public CountMetric(String callType, HashSet<AggregationParameter> aggregationParameters, int outputVersion) {
        super(callType, METRIC_TYPE, aggregationParameters, METRIC_UNIT, outputVersion);
    }

    public void recordValue(long value, Map<String, Object> params) {
        count.incrementAndGet();
    }

    public void resetValue() {
        count.set(0);
    }

    public void print(StringBuilder out) {
        out.append(key).append("} ").append(count).append("\n");
    }
}
