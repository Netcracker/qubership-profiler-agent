package com.netcracker.profiler.configuration.callfilters.params;

public class FilterOperatorContains extends FilterOperatorMatching {

    public FilterOperatorContains(String name, String value) {
        super(name, value);
    }

    public boolean evaluateCondition(String actualValue) {
        return actualValue.contains(value);
    }
}
