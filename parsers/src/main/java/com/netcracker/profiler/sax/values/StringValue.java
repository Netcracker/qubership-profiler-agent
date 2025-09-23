package com.netcracker.profiler.sax.values;

public class StringValue extends ValueHolder {
    public final String value;

    public StringValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }
}
