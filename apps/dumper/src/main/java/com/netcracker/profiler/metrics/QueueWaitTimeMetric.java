package com.netcracker.profiler.metrics;

import static com.netcracker.profiler.agent.FilterOperator.CALL_INFO_PARAM;

import com.netcracker.profiler.agent.CallInfo;
import com.netcracker.profiler.agent.MetricType;

import java.util.HashSet;
import java.util.Map;

public class QueueWaitTimeMetric extends AbstractHistogramMetric {

    private final long DEFAULT_VALUE_UNITS_IN_FIRST_BUCKET = 5;
    private final long DEFAULT_LOWEST_DISCERNIBLE_VALUE = 5;
    private final long DEFAULT_HIGHEST_TRACKABLE_VALUE = 10000000; //~2.7hours

    private static final MetricType METRIC_TYPE = MetricType.QUEUE_WAIT_TIME;
    private static final MetricUnit METRIC_UNIT = MetricUnit.MILLISECONDS;

    public QueueWaitTimeMetric(String callType, HashSet<AggregationParameter> aggregationParameters, Map<String, String> metricParameters, int outputVersion) {
        super(callType, METRIC_TYPE, aggregationParameters, METRIC_UNIT, outputVersion, BUCKET_SUFFIX);

        valueUnitsInFirstBucket = DEFAULT_VALUE_UNITS_IN_FIRST_BUCKET;
        lowestDiscernibleValue = DEFAULT_LOWEST_DISCERNIBLE_VALUE;
        highestTrackableValue = DEFAULT_HIGHEST_TRACKABLE_VALUE;

        parseHistogramParameters(metricParameters);
        initHistogram();
    }

    public void recordValue(long value, Map<String, Object> params) {
        CallInfo callInfo = (CallInfo) params.get(CALL_INFO_PARAM);

        recordValue(callInfo.queueWaitDuration);
    }

}
