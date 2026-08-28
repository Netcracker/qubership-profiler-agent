package com.netcracker.profiler.io;

public class TemporalRequestParams {
    public long now;
    public long serverUTC;
    public long clientUTC;
    public long timerangeFrom;
    public long timerangeTo;
    public long autoUpdate;

    public long durationFrom;
    public long durationTo;

    public TemporalRequestParams() {}
    public TemporalRequestParams(long now, long serverUTC, long clientUTC, long timerangeFrom, long timerangeTo, long autoUpdate, long durationFrom, long durationTo) {
        this.now = now;
        this.serverUTC = serverUTC;
        this.clientUTC = clientUTC;
        this.timerangeFrom = timerangeFrom;
        this.timerangeTo = timerangeTo;
        this.autoUpdate = autoUpdate;
        this.durationFrom = durationFrom;
        this.durationTo = durationTo;
    }
}
