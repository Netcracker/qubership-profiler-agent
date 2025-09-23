package com.netcracker.profiler.agent;

public enum MetricType {
    COUNT("count"),
    DURATION("duration"),
    CPU("cpu"),
    QUEUE_WAIT_TIME("queue_wait_time", "queue-wait-time"),
    TRANSACTIONS("transactions"),
    DISK_IO("disk_io", "disk-io"),
    NETWORK_IO("network_io", "network-io"),
    MEMORY("memory"),
    PROFILER_DIRTY_BUFFERS("profiler_dirty_buffers", "profiler-dirty-buffers"),
    PROFILER_EMPTY_BUFFERS("profiler_empty_buffers","profiler-empty-buffers");

    private String outputName;
    private String configName;

    MetricType(String name) {
        this.outputName = name;
        this.configName = name;
    }

    MetricType(String outputName, String configName) {
        this.outputName = outputName;
        this.configName = configName;
    }

    public String getOutputName() {
        return outputName;
    }

    public String getConfigName() {
        return configName;
    }

    public static MetricType getByConfigName(String name) {
        if (COUNT.getConfigName().equals(name)) {
            return COUNT;
        } else if (DURATION.getConfigName().equals(name)) {
            return DURATION;
        } else if (CPU.getConfigName().equals(name)) {
            return CPU;
        } else if (QUEUE_WAIT_TIME.getConfigName().equals(name)) {
            return QUEUE_WAIT_TIME;
        } else if (TRANSACTIONS.getConfigName().equals(name)) {
            return TRANSACTIONS;
        } else if (DISK_IO.getConfigName().equals(name)) {
            return DISK_IO;
        } else if (NETWORK_IO.getConfigName().equals(name)) {
            return NETWORK_IO;
        } else if (MEMORY.getConfigName().equals(name)) {
            return MEMORY;
        } else if (PROFILER_DIRTY_BUFFERS.getConfigName().equals(name)) {
            return PROFILER_DIRTY_BUFFERS;
        } else if (PROFILER_EMPTY_BUFFERS.getConfigName().equals(name)) {
            return PROFILER_EMPTY_BUFFERS;
        } else {
            return null;
        }
    }
}
