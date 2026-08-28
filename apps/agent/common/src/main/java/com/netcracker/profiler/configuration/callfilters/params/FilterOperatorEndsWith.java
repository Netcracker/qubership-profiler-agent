package com.netcracker.profiler.configuration.callfilters.params;

public class FilterOperatorEndsWith extends FilterOperatorMatching {

    public FilterOperatorEndsWith(String name, String value) {
        super(name, value);
    }

    public boolean evaluateCondition(String actualValue) {
        return actualValue.endsWith(value);
    }
}
