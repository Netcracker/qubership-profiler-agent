package org.qubership.profiler.agent;

public interface ThreadJMXProvider {
    public void updateThreadCounters(LocalState state);
}
