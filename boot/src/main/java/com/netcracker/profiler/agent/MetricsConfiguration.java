package com.netcracker.profiler.agent;

import java.util.List;

public interface MetricsConfiguration {

    String getName();

    Boolean isCustom();

    void setName(String name);

    void setIsCustom(String isCustom);

    String getMatchingClass();

    void setMatchingClass(String matchingClass);

    String getMatchingMethod();

    void setMatchingMethod(String matchingMethod);

    List<AggregationParameterDescriptor> getAggregationParameters();

    List<MetricsDescription> getMetrics();

    int getOutputVersion();
}
