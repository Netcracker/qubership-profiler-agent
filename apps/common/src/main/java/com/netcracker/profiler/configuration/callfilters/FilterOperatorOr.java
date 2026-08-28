package com.netcracker.profiler.configuration.callfilters;

import com.netcracker.profiler.agent.FilterOperator;

import java.util.Map;

public class FilterOperatorOr extends FilterOperatorLogical {

    public boolean evaluate(Map<String, Object> params) {
        for (FilterOperator ancestor : children) {
            if (ancestor.evaluate(params)) {
                return true;
            }
        }

        return false;
    }
}
