package com.netcracker.profiler.sax.raw;

import com.netcracker.profiler.util.ProfilerConstants;

/**
 * Collapses subsequent hiccups into one event
 */
public class SuspendLogCollapsingVisitor extends SuspendLogVisitor {
    private long prevDate = -1;
    private int prevDelay;

    public SuspendLogCollapsingVisitor(SuspendLogVisitor sv) {
        super(ProfilerConstants.PROFILER_V1, sv);
    }

    @Override
    public void visitHiccup(long date, int delay) {
        if (prevDate == date - delay) {
            prevDelay += delay;
            prevDate = date;
            return;
        }
        if (prevDate != -1) {
            super.visitHiccup(prevDate, prevDelay);
        }
        prevDate = date;
        prevDelay = delay;
    }

    @Override
    public void visitEnd() {
        if (prevDate != -1) {
            super.visitHiccup(prevDate, prevDelay);
            prevDate = -1;
        }
        super.visitEnd();
    }
}
