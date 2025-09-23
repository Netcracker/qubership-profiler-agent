package com.netcracker.profiler.agent;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.logging.Level;

public class ThreadJMXMemory implements ThreadJMXMemoryProvider {
    private static boolean DEBUG = Boolean.getBoolean(ThreadJMXMemory.class.getName() + ".debug");
    private static final ESCLogger logger = ESCLogger.getLogger(ThreadJMXMemory.class, (DEBUG ? Level.FINE : ESCLogger.ESC_LOG_LEVEL));

    private final static ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    private final static Method getThreadAllocatedBytes;

    static {
        try {
            getThreadAllocatedBytes = threadMXBean.getClass().getMethod("getThreadAllocatedBytes", long.class);
            getThreadAllocatedBytes.setAccessible(true);
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException("Unable to initialize ThreadJMXMemory provider", e);
        }
    }

    public void updateThreadCounters(LocalState state) {
        int now = TimerCache.timer;
        if (now - state.nextMemoryStamp < 0) return;
        try {
            state.memoryUsed = (Long) getThreadAllocatedBytes.invoke(threadMXBean, state.thread.getId());
        } catch (IllegalAccessException e) {
            if (DEBUG)
                logger.log(Level.FINE, "", e);
        } catch (InvocationTargetException e) {
            if (DEBUG)
                logger.log(Level.FINE, "", e);
        }
        state.nextMemoryStamp = now + ProfilerData.THREAD_MEMORY_MINIMAL_CALL_DURATION;
    }
}
