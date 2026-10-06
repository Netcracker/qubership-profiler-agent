package com.netcracker.profiler.configuration.callfilters.metrics;

import com.netcracker.profiler.agent.FilterOperator;
import com.netcracker.profiler.configuration.callfilters.metrics.condition.MathCondition;

public abstract class FilterOperatorMath implements FilterOperator {

    protected long constraintValue;
    protected MathCondition condition;

    public FilterOperatorMath(long constraintValue, MathCondition condition) {
        this.constraintValue = constraintValue;
        this.condition = condition;
    }

    public FilterOperatorMath() {}

    protected boolean evaluateCondition(long actualValue) {
        return condition.evaluateCondition(actualValue, constraintValue);
    }

    public long getConstraintValue() {
        return constraintValue;
    }

    public void setConstraintValue(long constraintValue) {
        this.constraintValue = constraintValue;
    }

    public MathCondition getCondition() {
        return condition;
    }

    public void setCondition(MathCondition condition) {
        this.condition = condition;
    }
}
