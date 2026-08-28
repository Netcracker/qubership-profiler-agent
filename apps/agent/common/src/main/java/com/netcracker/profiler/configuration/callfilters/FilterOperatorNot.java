package com.netcracker.profiler.configuration.callfilters;

import com.netcracker.profiler.agent.FilterOperator;

import java.util.Map;

public class FilterOperatorNot extends FilterOperatorLogical {

    public boolean evaluate(Map<String, Object> params) {
        for (FilterOperator ancestor : children) {
            return !ancestor.evaluate(params);
        }

        return false;
    }
}
