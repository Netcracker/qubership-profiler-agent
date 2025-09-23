package com.netcracker.profiler.metrics;

import com.netcracker.profiler.agent.SystemMetric;

public abstract class AbstractSystemMetric implements SystemMetric {
    protected String key;

    public AbstractSystemMetric(String name, MetricUnit metricUnit) {
        this.key = buildKey(name, metricUnit);
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public void setKey(String key) {
        this.key = key;
    }

    protected String buildKey(String name, MetricUnit metricUnit) {
        StringBuilder sb = new StringBuilder();
        return sb.append(name).append("_").append(metricUnit.getOutputValue()).append("{} ").toString();
    }


    protected abstract String getValue();

    @Override
    public void print(StringBuilder out) {
        out.append(key).append(getValue()).append("\n");
    }
}
