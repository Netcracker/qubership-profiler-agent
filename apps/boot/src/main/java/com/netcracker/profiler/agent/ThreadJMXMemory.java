package com.netcracker.profiler.agent;

import com.sun.management.ThreadMXBean;

import java.lang.management.ManagementFactory;
import java.util.logging.Level;

public class ThreadJMXMemory implements ThreadJMXMemoryProvider {
    private static boolean DEBUG = Boolean.getBoolean(ThreadJMXMemory.class.getName() + ".debug");
    private static final ESCLogger logger = ESCLogger.getLogger(ThreadJMXMemory.class, (DEBUG ? Level.FINE : ESCLogger.ESC_LOG_LEVEL));

    private final static ThreadMXBean threadMXBean = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();

    public void updateThreadCounters(LocalState state) {
        int now = TimerCache.timer;
        if (now - state.nextMemoryStamp < 0) return;
        state.memoryUsed = threadMXBean.getThreadAllocatedBytes(state.thread.getId());
        state.nextMemoryStamp = now + ProfilerData.THREAD_MEMORY_MINIMAL_CALL_DURATION;
    }
}
