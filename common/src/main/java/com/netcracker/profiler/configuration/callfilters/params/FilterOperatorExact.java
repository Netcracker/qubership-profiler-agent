package com.netcracker.profiler.configuration.callfilters.params;

public class FilterOperatorExact extends FilterOperatorMatching {

    public FilterOperatorExact(String name, String value) {
        super(name, value);
    }

    public boolean evaluateCondition(String actualValue) {
        return actualValue.equals(value);
    }
}
