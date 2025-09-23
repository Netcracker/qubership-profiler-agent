package com.netcracker.profiler.agent;

public interface BaseMetric {

    void setKey(String key);

    String getKey();

    void print(StringBuilder out);

}
