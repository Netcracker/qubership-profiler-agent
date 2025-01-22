package org.qubership.profiler.metrics;

import org.qubership.profiler.agent.CallInfo;
import org.qubership.profiler.agent.MetricType;
import org.qubership.profiler.dump.ThreadState;

import java.util.HashSet;
import java.util.Map;

import static org.qubership.profiler.agent.FilterOperator.CALL_INFO_PARAM;
import static org.qubership.profiler.agent.FilterOperator.THREAD_STATE_PARAM;

public class DiskIOMetric extends AbstractHistogramMetric {

    private final long DEFAULT_VALUE_UNITS_IN_FIRST_BUCKET = 8; //8Kb
    private final long DEFAULT_LOWEST_DISCERNIBLE_VALUE = 8; //8Kb
    private final long DEFAULT_HIGHEST_TRACKABLE_VALUE = 100000000; //~95gb

    private static final MetricType METRIC_TYPE = MetricType.DISK_IO;
    private static final MetricUnit METRIC_UNIT = MetricUnit.KILOBYTES;

    public DiskIOMetric(String callType, HashSet<AggregationParameter> aggregationParameters, Map<String, String> metricParameters, int outputVersion) {
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

        long fileRead = callInfo.fileRead - threadState.prevFileRead;
        long fileWritten = callInfo.fileWritten - threadState.prevFileWritten;

        recordValue((fileRead + fileWritten)/1024);
    }

}
