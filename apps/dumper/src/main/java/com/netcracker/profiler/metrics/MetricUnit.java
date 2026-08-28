package com.netcracker.profiler.metrics;

public enum MetricUnit {
    TOTAL("total"),
    MILLISECONDS("ms"),
    KILOBYTES("kb");

    private String outputValue;

    MetricUnit(String outputValue) {
        this.outputValue = outputValue;
    }

    public String getOutputValue() {
        return outputValue;
    }

    @Override
    public String toString() {
        return outputValue;
    }
}
