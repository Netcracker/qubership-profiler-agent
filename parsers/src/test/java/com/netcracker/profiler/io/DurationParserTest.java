package com.netcracker.profiler.io;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

public class DurationParserTest {
    @Test
    public void durationMillis() {
        assertEquals(42, DurationParser.parseDuration("42ms", 0));
        assertEquals(42, DurationParser.parseDuration("42 ms", 0));
        assertEquals(42, DurationParser.parseDuration(" 42 MS", 0));
        assertEquals(42, DurationParser.parseDuration(" 42 ms ", 0));
        assertEquals(42, DurationParser.parseDuration(" 42 millis ", 0));
    }

    @Test
    public void durationSeconds() {
        assertEquals(TimeUnit.SECONDS.toMillis(42), DurationParser.parseDuration("42s", 0));
        assertEquals(TimeUnit.SECONDS.toMillis(42), DurationParser.parseDuration("42seconds", 0));
    }

    @Test
    public void durationMinutes() {
        assertEquals(TimeUnit.MINUTES.toMillis(42), DurationParser.parseDuration("42m", 0));
        assertEquals(TimeUnit.MINUTES.toMillis(42), DurationParser.parseDuration("42minutes", 0));
        assertEquals(TimeUnit.MINUTES.toMillis(42), DurationParser.parseDuration("42Min", 0));
    }

    @Test
    public void durationHours() {
        assertEquals(TimeUnit.HOURS.toMillis(42), DurationParser.parseDuration("42h", 0));
        assertEquals(TimeUnit.HOURS.toMillis(42), DurationParser.parseDuration("42hours", 0));
    }

    @Test
    public void durationDays() {
        assertEquals(TimeUnit.DAYS.toMillis(42), DurationParser.parseDuration("42", 0));
        assertEquals(TimeUnit.DAYS.toMillis(42), DurationParser.parseDuration("42d", 0));
        assertEquals(TimeUnit.DAYS.toMillis(42), DurationParser.parseDuration("42days", 0));
    }

    @Test
    public void durationWeek() {
        assertEquals(TimeUnit.DAYS.toMillis(7), DurationParser.parseDuration("1 week", 0));
        assertEquals(TimeUnit.DAYS.toMillis(14), DurationParser.parseDuration("2w", 0));
    }

    @Test
    public void durationMonth() {
        assertEquals(TimeUnit.DAYS.toMillis(30), DurationParser.parseDuration("1 month", 0));
        assertEquals(TimeUnit.DAYS.toMillis(42 * 30), DurationParser.parseDuration("42mo", 0));
        assertEquals(TimeUnit.DAYS.toMillis(42 * 30), DurationParser.parseDuration("42months", 0));
    }

    @Test
    public void durationYear() {
        assertEquals(TimeUnit.DAYS.toMillis(364), DurationParser.parseDuration("1y", 0));
        assertEquals(TimeUnit.DAYS.toMillis(42 * 364), DurationParser.parseDuration("42years", 0));
    }

    @Test
    public void durationCombined() {
        assertEquals(TimeUnit.DAYS.toMillis(2) + TimeUnit.HOURS.toMillis(12) + TimeUnit.MINUTES.toMillis(7), DurationParser.parseDuration("2days 12hours 7m", 0));
        assertEquals(TimeUnit.DAYS.toMillis(2) - TimeUnit.HOURS.toMillis(12) + TimeUnit.MINUTES.toMillis(7), DurationParser.parseDuration("2days -12hours 7m", 0));
    }

    @Test
    public void durationUnparsable() {
        assertEquals(123456, DurationParser.parseDuration("abcde", 123456));
    }
}
