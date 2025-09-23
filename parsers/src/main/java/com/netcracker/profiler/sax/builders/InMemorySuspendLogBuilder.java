package com.netcracker.profiler.sax.builders;

import com.netcracker.profiler.io.SuspendLog;

public class InMemorySuspendLogBuilder extends SuspendLogBuilder {

    private boolean hasNotFinishedHiccup;

    public InMemorySuspendLogBuilder(int size, int maxSize) {
        super(size, maxSize, null);
    }

    @Override
    public void visitEnd() {}

    @Override
    public SuspendLog get() {
        return new SuspendLog(dates, delays, trueDelays, size);
    }

    public void visitNotFinishedHiccup(long date, int delay) {
        visitHiccupInternal(date, delay);
        hasNotFinishedHiccup = true;
    }

    public void visitFinishedHiccup(long date, int delay) {
        visitHiccupInternal(date, delay);
        hasNotFinishedHiccup = false;
    }

    private void visitHiccupInternal(long date, int delay) {
        if(hasNotFinishedHiccup) {
            int idx = size - 1;
            dates[idx] = date;
            delays[idx] = delay;
            if (trueDelays != null) {
                trueDelays[idx] = delay;
            }
        } else {
            visitHiccup(date, delay);
        }
    }
}
