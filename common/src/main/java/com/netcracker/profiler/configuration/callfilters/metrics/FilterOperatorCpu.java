package com.netcracker.profiler.configuration.callfilters.metrics;

import com.netcracker.profiler.agent.CallInfo;
import com.netcracker.profiler.configuration.callfilters.metrics.condition.MathCondition;
import com.netcracker.profiler.dump.ThreadState;

import java.util.Map;

public class FilterOperatorCpu extends FilterOperatorMath {

    public FilterOperatorCpu(long constraintValue, MathCondition condition) {
        super(constraintValue, condition);
    }

    public FilterOperatorCpu() {}

    @Override
    public boolean evaluate(Map<String, Object> params) {
        CallInfo callInfo = (CallInfo) params.get(CALL_INFO_PARAM);
        ThreadState threadState = (ThreadState) params.get(THREAD_STATE_PARAM);

        long cpuTime = callInfo.cpuTime - threadState.prevCpuTime;
        return evaluateCondition(cpuTime);
    }
}
