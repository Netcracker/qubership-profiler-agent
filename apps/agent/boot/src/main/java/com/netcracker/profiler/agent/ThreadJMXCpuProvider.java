package com.netcracker.profiler.agent;

public interface ThreadJMXCpuProvider {
    public void updateThreadCounters(LocalState state);
}
