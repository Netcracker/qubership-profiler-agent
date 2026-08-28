package com.netcracker.profiler.agent;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;

public class ThreadJMXWait implements ThreadJMXWaitProvider {
    private final static ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();

    public void updateThreadCounters(LocalState state) {
        int now = TimerCache.timer;
        if (now - state.nextWaitStamp < 0) return;
        final ThreadInfo info = threadMXBean.getThreadInfo(state.thread.getId());
        state.waitTime = info.getWaitedTime();
        state.nextWaitStamp = now + ProfilerData.THREAD_WAIT_MINIMAL_CALL_DURATION;
    }
}
