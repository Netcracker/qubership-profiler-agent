package com.netcracker.profiler.metrics;

import static com.netcracker.profiler.agent.FilterOperator.CALL_INFO_PARAM;
import static com.netcracker.profiler.agent.FilterOperator.THREAD_STATE_PARAM;

import com.netcracker.profiler.agent.CallInfo;
import com.netcracker.profiler.agent.MetricType;
import com.netcracker.profiler.dump.ThreadState;

import java.util.HashSet;
import java.util.Map;

public class CPUMetric extends AbstractHistogramMetric {

    private final long DEFAULT_VALUE_UNITS_IN_FIRST_BUCKET = 50;
    private final long DEFAULT_LOWEST_DISCERNIBLE_VALUE = 50;
    private final long DEFAULT_HIGHEST_TRACKABLE_VALUE = 100000000; //~27hours

    private static final MetricType METRIC_TYPE = MetricType.CPU;
    private static final MetricUnit METRIC_UNIT = MetricUnit.MILLISECONDS;

    public CPUMetric(String callType, HashSet<AggregationParameter> aggregationParameters, Map<String, String> metricParameters, int outputVersion) {
        super(callType, METRIC_TYPE, aggregationParameters, METRIC_UNIT, outputVersion, BUCKET_SUFFIX);

        valueUnitsInFirstBucket = DEFAULT_VALUE_UNITS_IN_FIRST_BUCKET;
        lowestDiscernibleValue = DEFAULT_LOWEST_DISCERNIBLE_VALUE;
        highestTrackableValue = DEFAULT_HIGHEST_TRACKABLE_VALUE;

        parseHistogramParameters(metricParameters);
        initHistogram();
    }

    public void recordValue(long value, Map<String, Object> params) {
        CallInfo callInfo = (CallInfo) params.get(CALL_INFO_PARAM);
        ThreadState threadState = (ThreadState) params.get(THREAD_STATE_PARAM);

        long cpuTime = callInfo.cpuTime - threadState.prevCpuTime;

        recordValue(cpuTime);
    }

}
