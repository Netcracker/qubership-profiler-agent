package org.qubership.profiler.agent;

import org.qubership.profiler.agent.LocalState;

public interface ThreadJMXMemoryProvider {
    public void updateThreadCounters(LocalState state);
}
