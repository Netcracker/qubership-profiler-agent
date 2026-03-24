package com.netcracker.common.models;

import com.netcracker.common.models.TimeRange;
import com.netcracker.utils.UnitTest;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.*;

@UnitTest
class TimeRangeTest {

    @Test
    void alignWithClient() {
        TimeRange range = TimeRange.ofEpochMilli(1689219120000L, 1689219200000L);
        assert(range.sysTime() > 1689011100000L);
        assertEquals(range, range.alignWithClient(-1));

        range = new TimeRange(Instant.ofEpochMilli(1689219120000L), Instant.ofEpochMilli(1689219200000L), 1689219120000L);
        assertEquals(range, range.alignWithClient(1689219120000L));
        assertEquals(TimeRange.ofEpochMilli(1689219118200L, 1689219201800L), range.alignWithClient(1689219120900L));
    }

    @Test
    void hash() {
        assertEquals("2023-07-13T03:32:00Z_2023-07-13T03:33:20Z", TimeRange.ofEpochMilli(1689219120000L, 1689219200000L).hash());
        assertEquals(-1779562928, TimeRange.ofEpochMilli(1689219120000L, 1689219200000L).hashCode());
        assertEquals("[2023-07-13T03:32:00Z - 2023-07-13T03:33:20Z]", TimeRange.ofEpochMilli(1689219120000L, 1689219200000L).toString());
    }


    @Test
    void generateHours() {
        assertHours(1, "2023-07-01T09:00:00Z", "2023-07-01T09:00:00Z",
                "2023-07-01T09:28:00Z", "2023-07-01T09:28:00Z");

        assertHours(2, "2023-07-01T09:00:00Z", "2023-07-01T10:00:00Z",
                "2023-07-01T09:28:00Z", "2023-07-01T10:58:00Z");

        assertHours(2, "2023-07-01T09:00:00Z", "2023-07-01T10:00:00Z",
                "2023-07-01T09:00:01Z", "2023-07-01T10:59:59Z");

        assertHours(4, "2023-07-01T08:00:00Z", "2023-07-01T11:00:00Z",
                "2023-07-01T08:59:01Z", "2023-07-01T11:01:00Z");
    }

    @Test
    void generateDays() {
        assertDays(1, "2023-07-01T00:00:00Z", "2023-07-01T00:00:00Z",
                "2023-07-01T09:28:00Z", "2023-07-01T09:28:00Z");

        assertDays(2, "2023-07-01T00:00:00Z", "2023-07-02T00:00:00Z",
                "2023-07-01T09:28:00Z", "2023-07-02T09:58:00Z");

        assertDays(2, "2023-07-01T00:00:00Z", "2023-07-02T00:00:00Z",
                "2023-07-01T08:59:01Z", "2023-07-02T10:01:00Z");

        assertDays(14, "2023-07-01T00:00:00Z", "2023-07-14T00:00:00Z",
                "2023-07-01T00:01:01Z", "2023-07-14T23:59:59Z");

    }

    @Test
    void generateDaysWithYesterday() {
        assertYesterDays(2, "2023-06-30T00:00:00Z", "2023-07-01T00:00:00Z",
                "2023-07-01T09:28:00Z", "2023-07-01T09:28:00Z");

        assertYesterDays(3, "2023-06-30T00:00:00Z", "2023-07-02T00:00:00Z",
                "2023-07-01T09:28:00Z", "2023-07-02T09:58:00Z");

        assertYesterDays(3, "2023-06-30T00:00:00Z", "2023-07-02T00:00:00Z",
                "2023-07-01T08:59:01Z", "2023-07-02T10:01:00Z");

        assertYesterDays(15, "2023-06-30T00:00:00Z", "2023-07-14T00:00:00Z",
                "2023-07-01T00:01:01Z", "2023-07-14T23:59:59Z");

    }

    private static void assertHours(int expectedHours, String expectedStart, String expectedEnd,
                                    String timeRangeStart, String timeRangeStartEnd) {
        List<Instant> hours = TimeRange.of(timeRangeStart, timeRangeStartEnd).hours();
        assertTsRange(expectedHours, expectedStart, expectedEnd, hours);
    }

    private static void assertDays(int expectedHours, String expectedStart, String expectedEnd,
                                   String timeRangeStart, String timeRangeStartEnd) {
        List<Instant> days = TimeRange.of(timeRangeStart, timeRangeStartEnd).days();
        assertTsRange(expectedHours, expectedStart, expectedEnd, days);
    }

    private static void assertYesterDays(int expectedHours, String expectedStart, String expectedEnd,
                                   String timeRangeStart, String timeRangeStartEnd) {
        List<Instant> days = TimeRange.of(timeRangeStart, timeRangeStartEnd).daysWithYesterday();
        assertTsRange(expectedHours, expectedStart, expectedEnd, days);
    }

    private static void assertTsRange(int expectedHours, String expectedStart, String expectedEnd, List<Instant> range) {
        assertEquals(expectedHours, range.size());
        assertEquals(Instant.parse(expectedStart), range.get(0));
        assertEquals(Instant.parse(expectedEnd), range.get(expectedHours-1));
    }
}