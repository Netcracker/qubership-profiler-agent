package com.netcracker.profiler.configuration.callfilters.metrics.condition;

public class LtOrEqCondition implements MathCondition {

    private static LtOrEqCondition instance;

    public static LtOrEqCondition getInstance() {
        if(instance == null) {
            instance = new LtOrEqCondition();
        }
        return instance;
    }

    @Override
    public boolean evaluateCondition(long v1, long v2) {
        return v1<=v2;
    }
}
