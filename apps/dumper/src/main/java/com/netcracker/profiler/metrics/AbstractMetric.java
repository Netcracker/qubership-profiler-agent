package com.netcracker.profiler.metrics;

import com.netcracker.profiler.agent.Metric;
import com.netcracker.profiler.agent.MetricType;
import com.netcracker.profiler.util.StringUtils;

import java.util.HashSet;

public abstract class AbstractMetric implements Metric {
    protected String key;
    protected int outputVersion;
    private volatile long updatedTime;

    public AbstractMetric(String callType, MetricType type, HashSet<AggregationParameter> aggregationParameters, MetricUnit metricUnit, int outputVersion, String suffix) {
        this.outputVersion = outputVersion;
        key = buildKey(callType, type, aggregationParameters, metricUnit, suffix);
    }

    public AbstractMetric(String callType, MetricType type, HashSet<AggregationParameter> aggregationParameters, MetricUnit metricUnit, int outputVersion) {
        this(callType, type, aggregationParameters, metricUnit, outputVersion, null);
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    public int getOutputVersion() {
        return outputVersion;
    }

    public void setOutputVersion(int outputVersion) {
        this.outputVersion = outputVersion;
    }

    public long getUpdatedTime() {
        return updatedTime;
    }

    public void resetUpdatedTime() {
        this.updatedTime = System.currentTimeMillis();
    }

    protected String buildKey(String callType, MetricType type, HashSet<AggregationParameter> aggregationParameters, MetricUnit metricUnit, String suffix) {
        if(outputVersion == 1) {
            return buildLegacyKey(callType, type, aggregationParameters, suffix);
        }

        StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append(callType).append("_").append(type.getOutputName()).append("_").append(metricUnit.getOutputValue());
        if(!StringUtils.isEmpty(suffix)) {
            stringBuilder.append("_").append(suffix);
        }
        stringBuilder.append("{");

        if(!aggregationParameters.isEmpty()) {
            for (AggregationParameter aggregationParameter : aggregationParameters) {
                stringBuilder.append(aggregationParameter.toString()).append(", ");
            }
            stringBuilder.delete(stringBuilder.length()-2, stringBuilder.length()); //delete last ", "
        }

        return stringBuilder.toString();
    }

    protected String buildKey(String callType, MetricType type, HashSet<AggregationParameter> aggregationParameters, MetricUnit metricUnit) {
        return buildKey(callType, type, aggregationParameters, metricUnit, null);
    }

    private String buildLegacyKey(String callType, MetricType type, HashSet<AggregationParameter> aggregationParameters, String suffix) {
        StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append("esc_").append(type.getOutputName()).append("s");
        if(!StringUtils.isEmpty(suffix)) {
            stringBuilder.append("_").append(suffix);
        }
        stringBuilder.append("{type=\"").append(callType).append("\"");

        for (AggregationParameter aggregationParameter : aggregationParameters) {
            stringBuilder.append(", ").append(aggregationParameter.toString());
        }

        return stringBuilder.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AbstractMetric that = (AbstractMetric) o;

        if (updatedTime != that.updatedTime) {
            return false;
        }
        return key.equals(that.key);
    }

    @Override
    public int hashCode() {
        int result = key.hashCode();
        result = 31 * result + (int) (updatedTime ^ (updatedTime >>> 32));
        return result;
    }
}
