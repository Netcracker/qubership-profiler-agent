package com.netcracker.profiler.sax.values;

import java.util.Objects;

public class StringValue extends ValueHolder {
    public final String value;

    public StringValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(value);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;

        StringValue that = (StringValue) o;
        return Objects.equals(value, that.value);
    }
}
