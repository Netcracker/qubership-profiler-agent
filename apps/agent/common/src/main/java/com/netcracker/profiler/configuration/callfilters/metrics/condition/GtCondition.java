package com.netcracker.profiler.configuration.callfilters.metrics.condition;

public class GtCondition implements MathCondition {

    private static GtCondition instance;

    public static GtCondition getInstance() {
        if(instance == null) {
            instance = new GtCondition();
        }
        return instance;
    }

    @Override
    public boolean evaluateCondition(long v1, long v2) {
        return v1>v2;
    }
}
