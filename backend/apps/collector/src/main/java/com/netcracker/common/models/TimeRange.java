package com.netcracker.common.models;

import com.fasterxml.jackson.annotation.JsonFormat;
import io.quarkus.logging.Log;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.ArrayList;
import java.util.List;

import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.HOURS;

public record TimeRange(
        @JsonFormat(without = {JsonFormat.Feature.READ_DATE_TIMESTAMPS_AS_NANOSECONDS, JsonFormat.Feature.WRITE_DATES_WITH_ZONE_ID}) // https://www.baeldung.com/jackson-serialize-dates
        Instant from,
        @JsonFormat(without = {JsonFormat.Feature.READ_DATE_TIMESTAMPS_AS_NANOSECONDS, JsonFormat.Feature.WRITE_DATES_WITH_ZONE_ID})
        Instant to,
        Long sysTime) {

    public String hash() {
        return String.format("%s_%s", from, to); // for search hash
    }

    @Override
    public String toString() {
        return String.format("[%s - %s]", from, to);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TimeRange range = (TimeRange) o;

        if (!from.equals(range.from)) return false;
        return to.equals(range.to);
    }

    @Override
    public int hashCode() {
        int result = from.hashCode();
        result = 31 * result + to.hashCode();
        return result;
    }

    public TimeRange alignWithClient(long clientUTC) {
        if (clientUTC == -1 || sysTime == null) {
            return this;
        }
        long clientServerTimeDiff = (long) (Math.abs(clientUTC - sysTime) * 2);
        return new TimeRange(from.minusMillis(clientServerTimeDiff), to.plusMillis(clientServerTimeDiff), sysTime);

    }

    public static TimeRange ofEpochMilli(long from, long to) {
        return new TimeRange(Instant.ofEpochMilli(from), Instant.ofEpochMilli(to), System.currentTimeMillis());
    }

    public static TimeRange of(Instant from, Instant to) {
        return new TimeRange(from, to, System.currentTimeMillis());
    }

    public static TimeRange of(String from, String to) {
        return new TimeRange(Instant.parse(from), Instant.parse(to), System.currentTimeMillis());
    }

    // days for time range:  [d1: {start}..][d2][d3:..{end} ] (without next day)
    public List<Instant> days() {
        return days(from, to);
    }

    // days for time range:  [d0: {yesterday}][d1: {start}..][d2][d3:..{end} ] (without next day)
    public List<Instant> daysWithYesterday() {
        return days(from.minus(1, DAYS), to);
    }

    public static List<Instant> days(Instant from, Instant to) {
        return range(from, to, DAYS,31);  // safe measure: 1 month
    }

    // hours for time range, inclusive:  [h1: {start}..][h2][h3][h4][h5:..{end} ][h6] (without next hour)
    public List<Instant> hours() {
        return range(from, to, HOURS, 24 * 31);  // safe measure: 1 month
    }

    static List<Instant> range(Instant from, Instant to, TemporalUnit step, int limit) {
        List<Instant> list = new ArrayList<>();
        if (from.isAfter(to)) {
            return list;
        }
        long duration = limit * step.getDuration().toMillis();
        if (to.toEpochMilli() - from.toEpochMilli() > duration) { // safe measure
            Log.warnf("The range [%s - %s] exceed limits of %d %s", from, to, limit, step.toString());
            from = to.minus(limit, step);
        }

        var t = from.truncatedTo(step);
        var i = 0;
        while (t.isBefore(to)) {
            i++;
            if (i > limit) break; // safe measure: 1 year
            list.add(t);
            t = t.plus(1, step);
        }
        return list;
    }

    /**
     * Calculates the difference between two Instants using the specified
     * ChronoUnit.
     *
     * This method returns the number of units (e.g., days, hours, minutes) between
     * the `from` and `to` timestamps.
     *
     * Parameters:
     * - from: the starting Instant
     * - to: the ending Instant
     * - unit: the ChronoUnit used to calculate the difference (e.g.,
     * ChronoUnit.DAYS, ChronoUnit.HOURS)
     *
     * Returns:
     * - The number of units between the two instants.
     *
     * Example:
     * Instant now = Instant.now();
     * Instant yesterday = now.minus(1, ChronoUnit.DAYS);
     * long diffDays = delta(yesterday, now, ChronoUnit.DAYS); // returns 1
     */
    public static long delta(Instant from, Instant to, ChronoUnit unit) {
        return unit.between(from, to);
    }

    public boolean isValid() {
        return from != null && to != null && from().isBefore(to);
    }

}
