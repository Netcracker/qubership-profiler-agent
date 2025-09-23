package com.netcracker.profiler.configuration.callfilters.metrics.condition;

public class LtCondition implements MathCondition {

    private static LtCondition instance;

    public static LtCondition getInstance() {
        if(instance == null) {
            instance = new LtCondition();
        }
        return instance;
    }

    @Override
    public boolean evaluateCondition(long v1, long v2) {
        return v1<v2;
    }
}
