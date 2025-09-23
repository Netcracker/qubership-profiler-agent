package com.netcracker.profiler.configuration;

import com.netcracker.profiler.agent.ProfilerProperty;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class ProfilerPropertyImpl implements ProfilerProperty {

    private String singleValue; //firstValue
    private Set<String> multipleValues = new HashSet<>();

    @Override
    public void addValue(String value) {
        if(singleValue == null) {
            singleValue = value;
        }
        multipleValues.add(value);
    }

    @Override
    public void addValues(Collection<String> values) {
        if(values == null || values.isEmpty()) {
            return;
        }
        if(singleValue == null) {
            singleValue = values.iterator().next();
        }
        multipleValues.addAll(values);
    }

    @Override
    public void setValue(String value) {
        singleValue = value;
        multipleValues.clear();
        multipleValues.add(value);
    }

    @Override
    public void setValues(Collection<String> values) {
        if(values == null || values.isEmpty()) {
            singleValue = null;
            multipleValues.clear();
        }
        singleValue = values.iterator().next();
        multipleValues = new HashSet<>(values);
    }

    @Override
    public String getSingleValue() {
        return singleValue;
    }

    @Override
    public Set<String> getMultipleValues() {
        return multipleValues;
    }
}
