package com.netcracker.profiler.configuration;

import com.netcracker.profiler.agent.AggregationParameterDescriptor;
import com.netcracker.profiler.agent.FilterOperator;
import com.netcracker.profiler.agent.MetricsConfiguration;
import com.netcracker.profiler.agent.MetricsDescription;
import com.netcracker.profiler.configuration.callfilters.FilterOperatorAnd;

import java.util.LinkedList;
import java.util.List;

public class MetricsConfigurationImpl implements MetricsConfiguration {

    private String name;
    private String matchingClass;
    private String matchingMethod;
    private Boolean isCustom;
    private int outputVersion;
    private List<AggregationParameterDescriptor> aggregationParameters = new LinkedList<AggregationParameterDescriptor>();
    private List<MetricsDescription> metrics = new LinkedList<MetricsDescription>();
    private FilterOperator filter = new FilterOperatorAnd();

    public String getName() {
        return name;
    }

    public Boolean isCustom() {
        return isCustom;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setIsCustom(String isCustom) {
        this.isCustom = "true".equals(isCustom);
    }

    public int getOutputVersion() {
        return outputVersion;
    }

    public void setOutputVersion(int outputVersion) {
        this.outputVersion = outputVersion;
    }

    public String getMatchingClass() {
        return matchingClass;
    }

    public void setMatchingClass(String matchingClass) {
        this.matchingClass = matchingClass;
    }

    public String getMatchingMethod() {
        return matchingMethod;
    }

    public void setMatchingMethod(String matchingMethod) {
        this.matchingMethod = matchingMethod;
    }

    public List<AggregationParameterDescriptor> getAggregationParameters() {
        return aggregationParameters;
    }

    public List<MetricsDescription> getMetrics() {
        return metrics;
    }


    public FilterOperator getFilter() {
        return filter;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MetricsConfigurationImpl that = (MetricsConfigurationImpl) o;

        if (outputVersion != that.outputVersion) return false;
        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        if (matchingClass != null ? !matchingClass.equals(that.matchingClass) : that.matchingClass != null)
            return false;
        if (matchingMethod != null ? !matchingMethod.equals(that.matchingMethod) : that.matchingMethod != null)
            return false;
        if (isCustom != null ? !isCustom.equals(that.isCustom) : that.isCustom != null) return false;
        if (aggregationParameters != null ? !aggregationParameters.equals(that.aggregationParameters) : that.aggregationParameters != null)
            return false;
        if (metrics != null ? !metrics.equals(that.metrics) : that.metrics != null) return false;
        return filter != null ? filter.equals(that.filter) : that.filter == null;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (matchingClass != null ? matchingClass.hashCode() : 0);
        result = 31 * result + (matchingMethod != null ? matchingMethod.hashCode() : 0);
        result = 31 * result + (isCustom != null ? isCustom.hashCode() : 0);
        result = 31 * result + outputVersion;
        result = 31 * result + (aggregationParameters != null ? aggregationParameters.hashCode() : 0);
        result = 31 * result + (metrics != null ? metrics.hashCode() : 0);
        result = 31 * result + (filter != null ? filter.hashCode() : 0);
        return result;
    }
}
