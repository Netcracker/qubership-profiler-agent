package com.netcracker.profiler.metrics;

import gnu.trove.set.hash.THashSet;
import org.apache.commons.lang.StringUtils;

public class AggregationParameter {
    private String aggregationParamName;
    private THashSet<String> aggregationParamValues;

    public AggregationParameter(String aggregationParamName, THashSet<String> aggregationParamValues) {
        this.aggregationParamName = aggregationParamName;
        this.aggregationParamValues = aggregationParamValues;
    }

    @Override
    public String toString() {
        return aggregationParamName + "=\"" + StringUtils.join(aggregationParamValues, ", ").replace("\"", "") + "\"";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AggregationParameter that = (AggregationParameter) o;

        if (aggregationParamName != null ? !aggregationParamName.equals(that.aggregationParamName) : that.aggregationParamName != null) {
            return false;
        }
        return aggregationParamValues != null ? aggregationParamValues.equals(that.aggregationParamValues) : that.aggregationParamValues == null;
    }

    @Override
    public int hashCode() {
        int result = aggregationParamName != null ? aggregationParamName.hashCode() : 0;
        result = 31 * result + (aggregationParamValues != null ? aggregationParamValues.hashCode() : 0);
        return result;
    }
}
