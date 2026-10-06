package com.netcracker.profiler.configuration;


public class CallType {

    private MetricsConfigurationImpl metricsConfigurationImpl = null;

    public CallType() {
        metricsConfigurationImpl = new MetricsConfigurationImpl();
    }

    public MetricsConfigurationImpl getMetricsConfigurationImpl() {
        return metricsConfigurationImpl;
    }

}
