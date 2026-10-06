package com.netcracker.profiler.agent;

public enum HistogramIterationType {
    PERCENTILES("percentiles"), LINEAR_BUCKET_VALUES("linearBucketValues"), LOGARITHMIC_BUCKET_VALUES("logarithmicBucketValues"),
    ALL_VALUES("allValues"), RECORDED_VALUES("recordedValues");

    private String configName;

    HistogramIterationType(String configName) {
        this.configName = configName;
    }

    public String getConfigName() {
        return configName;
    }

    public static HistogramIterationType getByConfigName(String name) {
        if (PERCENTILES.getConfigName().equals(name)) {
            return PERCENTILES;
        } else if (LINEAR_BUCKET_VALUES.getConfigName().equals(name)) {
            return LINEAR_BUCKET_VALUES;
        } else if (LOGARITHMIC_BUCKET_VALUES.getConfigName().equals(name)) {
            return LOGARITHMIC_BUCKET_VALUES;
        } else if (ALL_VALUES.getConfigName().equals(name)) {
            return ALL_VALUES;
        } else if (RECORDED_VALUES.getConfigName().equals(name)) {
            return RECORDED_VALUES;
        } else {
            return null;
        }
    }
}
