package com.netcracker.profiler.sax.builders;

import com.netcracker.profiler.io.SuspendLog;

public class InMemorySuspendLogBuilderStub extends InMemorySuspendLogBuilder {

    public InMemorySuspendLogBuilderStub() {
        super(0, 0);
    }

    @Override
    public void visitNotFinishedHiccup(long date, int delay) {}

    @Override
    public void visitFinishedHiccup(long date, int delay) {}

    @Override
    public void visitHiccup(long date, int delay) {}

    @Override
    public SuspendLog get() {
        return SuspendLog.EMPTY;
    }
}
