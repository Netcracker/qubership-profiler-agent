package com.netcracker.profiler.configuration.callfilters.metrics;

import com.netcracker.profiler.agent.CallInfo;
import com.netcracker.profiler.configuration.callfilters.metrics.condition.MathCondition;

import java.util.Map;

public class FilterOperatorQueueWaitTime extends FilterOperatorMath {

    public FilterOperatorQueueWaitTime(long constraintValue, MathCondition condition) {
        super(constraintValue, condition);
    }

    public FilterOperatorQueueWaitTime() {}

    @Override
    public boolean evaluate(Map<String, Object> params) {
        CallInfo callInfo = (CallInfo) params.get(CALL_INFO_PARAM);

        return evaluateCondition(callInfo.queueWaitDuration);
    }
}
