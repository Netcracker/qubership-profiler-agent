package org.qubership.profiler.configuration.callfilters.metrics;

import org.qubership.profiler.agent.CallInfo;
import org.qubership.profiler.configuration.callfilters.metrics.condition.MathCondition;
import org.qubership.profiler.dump.ThreadState;

import java.util.Map;

public class FilterOperatorMemory extends FilterOperatorMath {

    public FilterOperatorMemory(long constraintValue, MathCondition condition) {
        super(constraintValue, condition);
    }

    public FilterOperatorMemory() {}

    @Override
    public boolean evaluate(Map<String, Object> params) {
        CallInfo callInfo = (CallInfo) params.get(CALL_INFO_PARAM);
        ThreadState threadState = (ThreadState) params.get(THREAD_STATE_PARAM);

        long memoryUsed = (callInfo.memoryUsed - threadState.prevMemoryUsed)/1024;
        return evaluateCondition(memoryUsed);
    }
}
