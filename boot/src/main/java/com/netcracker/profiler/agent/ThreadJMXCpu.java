package com.netcracker.profiler.agent;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

public class ThreadJMXCpu implements ThreadJMXCpuProvider {
    private final static ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();

    public void updateThreadCounters(LocalState state) {
        int now = TimerCache.timer;
        if (now - state.nextCpuStamp < 0) return;
        state.cpuTime = threadMXBean.getThreadCpuTime(state.thread.getId()) / 1000000; // convert nano seconds to milli seconds
        state.nextCpuStamp = now + ProfilerData.THREAD_CPU_MINIMAL_CALL_DURATION;
    }
}
