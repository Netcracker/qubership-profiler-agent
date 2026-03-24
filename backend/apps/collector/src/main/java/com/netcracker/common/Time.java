package com.netcracker.common;

import com.netcracker.common.models.TimeRange;
import jakarta.inject.Singleton;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import static java.time.temporal.ChronoUnit.DAYS;

@Singleton
public class Time {
    private Instant curTime; // only for test purposes

    private Set<Consumer<Instant>> listeners = new HashSet();

    public Instant now() {
        if (curTime == null) {
            return Instant.now();
        }
        return curTime;
    }

    public long currentTimeMillis() {
        return now().toEpochMilli();
    }

    public Instant curMinute() {
        return now().truncatedTo(ChronoUnit.MINUTES);
    }

    public Instant today() {
        return now().truncatedTo(ChronoUnit.DAYS);
    }


    public List<Instant> days(Instant t) {
        return List.of(t.truncatedTo(DAYS), t.truncatedTo(DAYS).minus(1, DAYS));
    }

    public TimeRange ofLast(long amount, TemporalUnit unit) {
        return new TimeRange(now().minus(amount, unit), now(), System.currentTimeMillis());
    }

    public TimeRange tillNow() {
        return TimeRange.of(Instant.EPOCH, now().plus(10, ChronoUnit.MINUTES));
    }

    public TimeRange rangeFrom(Instant fromTime) {
        return TimeRange.of(fromTime, now());
    }

    public TimeRange range1Day() {
        return TimeRange.of(now().minus(1, ChronoUnit.DAYS), now());
    }

    public void setTime(Instant now) { // only for test purposes
        this.curTime = now;
        for (var l: listeners) {
            l.accept(curTime);
        }
    }

    public void addListener(Consumer<Instant> listener) { // only for test purposes, to keep track of `time` changes
        this.listeners.add(listener);
    }

    public List<Long> toEpochMilli(List<Instant> t) {
        return t.stream().map(Instant::toEpochMilli).toList();
    }

}
