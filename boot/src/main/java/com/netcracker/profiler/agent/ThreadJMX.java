package com.netcracker.profiler.agent;

public class ThreadJMX implements ThreadJMXProvider {
    private final ThreadJMXCpuProvider cpu;
    private final ThreadJMXWaitProvider wait;
    private final ThreadJMXMemoryProvider memory;

    public ThreadJMX(String[] classNames) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        cpu = (ThreadJMXCpuProvider) Class.forName(classNames[0]).newInstance();
        wait = (ThreadJMXWaitProvider) Class.forName(classNames[1]).newInstance();
        memory = (ThreadJMXMemoryProvider) Class.forName(classNames[2]).newInstance();
    }

    public void updateThreadCounters(LocalState state) {
        cpu.updateThreadCounters(state);
        wait.updateThreadCounters(state);
        memory.updateThreadCounters(state);
    }
}
