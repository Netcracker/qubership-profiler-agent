package com.netcracker.profiler.io;

public class DurationFiltererImpl implements DurationFilterer {

    private final long durationFrom;
    private final long durationTo;

    public DurationFiltererImpl(long durationFrom, long durationTo) {
        this.durationFrom = durationFrom;
        this.durationTo = durationTo;
    }

    @Override
    public long getDurationFrom() {
        return durationFrom;
    }

    @Override
    public long getDurationTo() {
        return durationTo;
    }

    @Override
    public boolean filter(Call call) {
        return call.duration >= durationFrom && call.duration < durationTo;
    }
}
