package com.netcracker.profiler.configuration.callfilters.metrics;

import com.netcracker.profiler.configuration.callfilters.metrics.condition.MathCondition;

import java.util.Map;

public class FilterOperatorDuration extends FilterOperatorMath {

    public FilterOperatorDuration(long constraintValue, MathCondition condition) {
        super(constraintValue, condition);
    }

    public FilterOperatorDuration() {}

    @Override
    public boolean evaluate(Map<String, Object> params) {
        Long duration = (Long) params.get(DURATION_PARAM);
        return evaluateCondition(duration);
    }
}
