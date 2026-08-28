package com.netcracker.profiler.agent;

import java.util.Map;

public interface Configuration_01 extends Configuration {
    public Map<String, ParameterInfo> getParametersInfo();

    public ParameterInfo getParameterInfo(String name);

    public long getLogMaxAge();

    public long getLogMaxSize();
}
