package com.netcracker.profiler.configuration.callfilters.metrics.condition;

public class EqCondition implements MathCondition {

    private static EqCondition instance;

    public static EqCondition getInstance() {
        if(instance == null) {
            instance = new EqCondition();
        }
        return instance;
    }

    @Override
    public boolean evaluateCondition(long v1, long v2) {
        return v1==v2;
    }
}
