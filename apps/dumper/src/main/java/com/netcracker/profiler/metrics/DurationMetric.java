package com.netcracker.profiler.metrics;

import com.netcracker.profiler.agent.MetricType;

import java.util.HashSet;
import java.util.Map;

public class DurationMetric extends AbstractHistogramMetric {

    private final long DEFAULT_VALUE_UNITS_IN_FIRST_BUCKET = 100;
    private final long DEFAULT_LOWEST_DISCERNIBLE_VALUE = 100;
    private final long DEFAULT_HIGHEST_TRACKABLE_VALUE = 100000000; //~27hours

    private static final MetricType METRIC_TYPE = MetricType.DURATION;
    private static final MetricUnit METRIC_UNIT = MetricUnit.MILLISECONDS;

    public DurationMetric(String callType, HashSet<AggregationParameter> aggregationParameters, Map<String, String> metricParameters, int outputVersion) {
        super(callType, METRIC_TYPE, aggregationParameters, METRIC_UNIT, outputVersion, BUCKET_SUFFIX);

        valueUnitsInFirstBucket = DEFAULT_VALUE_UNITS_IN_FIRST_BUCKET;
        lowestDiscernibleValue = DEFAULT_LOWEST_DISCERNIBLE_VALUE;
        highestTrackableValue = DEFAULT_HIGHEST_TRACKABLE_VALUE;

        parseHistogramParameters(metricParameters);
        initHistogram();
    }

    public void recordValue(long value, Map<String, Object> params) {
        recordValue(value);
    }

}
