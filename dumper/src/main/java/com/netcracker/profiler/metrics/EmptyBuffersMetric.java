package com.netcracker.profiler.metrics;

import com.netcracker.profiler.agent.ProfilerData;

public class EmptyBuffersMetric extends AbstractSystemMetric {

    private static final MetricUnit METRIC_UNIT = MetricUnit.TOTAL;

    public EmptyBuffersMetric(String name) {
        super(name, METRIC_UNIT);
    }

    @Override
    protected String getValue() {
        return String.valueOf(ProfilerData.emptyBuffers.size());
    }
}
