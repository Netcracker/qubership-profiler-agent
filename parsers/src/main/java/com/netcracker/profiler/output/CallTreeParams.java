package com.netcracker.profiler.output;

import java.util.Collections;
import java.util.Map;

public class CallTreeParams {
    private final Map values;

    public CallTreeParams() {
        this(Collections.emptyMap());
    }

    public CallTreeParams(Map values) {
        this.values = values;
    }

    public String get(String name) {
        Object value = values.get(name);
        if (value == null || !(value instanceof String)) {
            return null;
        }
        return (String) value;
    }

}
