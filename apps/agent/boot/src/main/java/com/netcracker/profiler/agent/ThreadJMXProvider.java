package com.netcracker.profiler.agent;

public interface ThreadJMXProvider {
    public void updateThreadCounters(LocalState state);
}
