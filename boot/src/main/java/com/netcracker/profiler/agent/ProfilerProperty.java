package com.netcracker.profiler.agent;

import java.util.Collection;
import java.util.Set;

public interface ProfilerProperty {
    void addValue(String value);
    void addValues(Collection<String> values);
    void setValue(String value);
    void setValues(Collection<String> values);
    String getSingleValue();
    Set<String> getMultipleValues();
}
