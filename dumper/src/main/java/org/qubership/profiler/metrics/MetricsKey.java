package org.qubership.profiler.metrics;

import org.qubership.profiler.agent.*;

import java.util.HashSet;

public class MetricsKey {

    private String callType;
    private MetricType metricType;
    private HashSet<AggregationParameter> aggregationParams;
    private boolean isCustom;

    public MetricsKey(String callType, MetricType metricType, HashSet<AggregationParameter> aggregationParams, boolean isCustom) {
        this.callType = callType;
        this.metricType = metricType;
        this.aggregationParams = aggregationParams;
        this.isCustom = isCustom;
    }

    public boolean isCustom() {
        return isCustom;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MetricsKey that = (MetricsKey) o;

        if (isCustom != that.isCustom) {
            return false;
        }
        if (callType != null ? !callType.equals(that.callType) : that.callType != null) {
            return false;
        }
        if (metricType != null ? !metricType.equals(that.metricType) : that.metricType != null) {
            return false;
        }
        return aggregationParams != null ? aggregationParams.equals(that.aggregationParams) : that.aggregationParams == null;
    }

    @Override
    public int hashCode() {
        int result = callType != null ? callType.hashCode() : 0;
        result = 31 * result + (metricType != null ? metricType.hashCode() : 0);
        result = 31 * result + (aggregationParams != null ? aggregationParams.hashCode() : 0);
        result = 31 * result + (isCustom ? 1 : 0);
        return result;
    }
}
