package com.netcracker.profiler.configuration.callfilters.metrics.condition;

public interface MathCondition {
    boolean evaluateCondition(long v1, long v2);
}
