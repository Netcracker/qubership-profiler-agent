package com.netcracker.common.models;

import java.time.Duration;

public record DurationRange(Duration from, Duration to) {

    public boolean isValid() {
        return (from.isPositive() || from.isZero()) && to.isPositive() && from.toMillis() < to.toMillis();
    }

    public boolean inRange(long durationMs) {
        return durationMs >= from.toMillis() && durationMs <= to.toMillis();
    }

    public String hash() {
        return String.format("%s_%s", from, to); // for search hash
    }

    @Override
    public String toString() {
        return String.format("[%s-%s]", from, to);
    }

    public static DurationRange ofMillis(long from, long to) {
        return new DurationRange(Duration.ofMillis(from), Duration.ofMillis(to));
    }

    public static DurationRange ofSeconds(long from, long to) {
        return new DurationRange(Duration.ofSeconds(from), Duration.ofSeconds(to));
    }

    public static DurationRange of(Duration from, Duration to) {
        return new DurationRange(from, to);
    }
}
