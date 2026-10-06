package com.netcracker.profiler.configuration.callfilters.params;

public class FilterOperatorStartsWith extends FilterOperatorMatching {

    public FilterOperatorStartsWith(String name, String value) {
        super(name, value);
    }

    public boolean evaluateCondition(String actualValue) {
        return actualValue.startsWith(value);
    }
}
