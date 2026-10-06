package com.netcracker.profiler.sax.raw;

import com.netcracker.profiler.util.ProfilerConstants;

public class SuspendLogSummary extends SuspendLogVisitor {
    public int minDelay = Integer.MAX_VALUE;
    public int maxDelay = Integer.MIN_VALUE;
    public int totalDelay;
    public int totalEvents;

    public SuspendLogSummary(SuspendLogVisitor sv) {
        super(ProfilerConstants.PROFILER_V1, sv);
    }

    @Override
    public void visitHiccup(long date, int delay) {
        minDelay = Math.min(minDelay, delay);
        maxDelay = Math.max(maxDelay, delay);
        totalDelay += delay;
        totalEvents++;
        super.visitHiccup(date, delay);
    }

    @Override
    public String toString() {
        return "SuspendLogHashCode{" +
                "minDelay=" + minDelay +
                ", maxDelay=" + maxDelay +
                ", totalDelay=" + totalDelay +
                ", totalEvents=" + totalEvents +
                '}';
    }
}
