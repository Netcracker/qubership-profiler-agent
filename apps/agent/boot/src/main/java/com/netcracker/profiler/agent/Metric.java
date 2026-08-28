package com.netcracker.profiler.agent;

import java.util.Map;

public interface Metric extends BaseMetric {

    void recordValue(long value, Map<String, Object> params);

    void resetValue();

    long getUpdatedTime();

    void resetUpdatedTime();

    public void setOutputVersion(int outputVersion);

    public int getOutputVersion();

}
