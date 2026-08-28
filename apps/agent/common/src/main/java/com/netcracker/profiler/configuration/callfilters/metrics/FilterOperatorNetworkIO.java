package com.netcracker.profiler.configuration.callfilters.metrics;

import com.netcracker.profiler.agent.CallInfo;
import com.netcracker.profiler.configuration.callfilters.metrics.condition.MathCondition;
import com.netcracker.profiler.dump.ThreadState;

import java.util.Map;

public class FilterOperatorNetworkIO extends FilterOperatorMath {

    public FilterOperatorNetworkIO(long constraintValue, MathCondition condition) {
        super(constraintValue, condition);
    }

    public FilterOperatorNetworkIO() {}

    @Override
    public boolean evaluate(Map<String, Object> params) {
        CallInfo callInfo = (CallInfo) params.get(CALL_INFO_PARAM);
        ThreadState threadState = (ThreadState) params.get(THREAD_STATE_PARAM);

        long netRead = callInfo.netRead - threadState.prevNetRead;
        long netWritten = callInfo.netWritten - threadState.prevNetWritten;
        long networkIO = (netRead + netWritten)/1024;

        return evaluateCondition(networkIO);
    }
}
