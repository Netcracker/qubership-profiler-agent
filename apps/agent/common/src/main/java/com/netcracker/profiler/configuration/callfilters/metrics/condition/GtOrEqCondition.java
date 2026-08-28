package com.netcracker.profiler.configuration.callfilters.metrics.condition;

public class GtOrEqCondition implements MathCondition {

    private static GtOrEqCondition instance;

    public static GtOrEqCondition getInstance() {
        if(instance == null) {
            instance = new GtOrEqCondition();
        }
        return instance;
    }

    @Override
    public boolean evaluateCondition(long v1, long v2) {
        return v1>=v2;
    }
}
