package com.netcracker.profiler.configuration.callfilters.metrics;

import com.netcracker.profiler.agent.CallInfo;
import com.netcracker.profiler.configuration.callfilters.metrics.condition.MathCondition;
import com.netcracker.profiler.dump.ThreadState;

import java.util.Map;

public class FilterOperatorDiskIO extends FilterOperatorMath {

    public FilterOperatorDiskIO(long constraintValue, MathCondition condition) {
        super(constraintValue, condition);
    }

    public FilterOperatorDiskIO() {}

    @Override
    public boolean evaluate(Map<String, Object> params) {
        CallInfo callInfo = (CallInfo) params.get(CALL_INFO_PARAM);
        ThreadState threadState = (ThreadState) params.get(THREAD_STATE_PARAM);

        long fileRead = callInfo.fileRead - threadState.prevFileRead;
        long fileWritten = callInfo.fileWritten - threadState.prevFileWritten;
        long diskIO = (fileRead + fileWritten)/1024;

        return evaluateCondition(diskIO);
    }
}
