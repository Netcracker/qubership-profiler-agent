package com.netcracker.persistence.utils;

import com.netcracker.utils.UnitTest;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.*;

@UnitTest
class MiscUtilTest {
    @Test
    void testNormalizeParam_valid() {
        assertEquals("xb3traceid", MiscUtil.normalizeParam("X-B3-TraceId"));
        assertEquals("jobjmsconnection", MiscUtil.normalizeParam("job.jms.connection"));
        assertEquals("foobarabc", MiscUtil.normalizeParam("FOO.BAR-abc"));
        assertEquals("onlyunderscores", MiscUtil.normalizeParam("___only__underscores__"));
        assertEquals("param123", MiscUtil.normalizeParam("param-123"));
        assertEquals("traceid123", MiscUtil.normalizeParam("trace.id.123"));
        assertEquals("param1", MiscUtil.normalizeParam("param.1"));
    }

    @Test
    void testNormalizeParam_null() {
        assertThrows(IllegalArgumentException.class, () -> MiscUtil.normalizeParam(null));
    }

    @Test
    void testNormalizeParam_emptyAfterStripping() {
        assertThrows(IllegalArgumentException.class, () -> MiscUtil.normalizeParam("%%%"));
    }

    @Test
    void testNormalizeParam_startsWithDigit() {
        assertThrows(IllegalArgumentException.class, () -> MiscUtil.normalizeParam("1abc"));
    }

    @Test
    void testNormalizeParamList_valid() {
        assertEquals("abc,xyz", MiscUtil.normalizeParamList("ab.c, xy.z"));
    }

    @Test
    void testNormalizeParamList_empty() {
        assertEquals("", MiscUtil.normalizeParamList(""));
    }

    @Test
    void testNormalizeParamList_null() {
        assertEquals("", MiscUtil.normalizeParamList(null));
    }

    @Test
    void testContainsValuesInMap_match() {
        Map<String, List<String>> map = Map.of("jms.replyto", List.of("GET", "POST"));
        Map<String, List<String>> subMap = Map.of("jms.replyto", List.of("GET"));
        assertTrue(MiscUtil.containsValuesInMap(map, subMap));
    }

    @Test
    void testContainsValuesInMap_noMatch() {
        Map<String, List<String>> map = Map.of("jms.replyto", List.of("GET", "POST"));
        Map<String, List<String>> subMap = Map.of("jms.replyto", List.of("PUT"));
        assertFalse(MiscUtil.containsValuesInMap(map, subMap));
    }

    @Test
    void testGetQuotedStringOfList() {
        List<String> input = List.of("dev", "test");
        String result = MiscUtil.getQuotedStringOfList(input);
        assertEquals("'dev','test'", result);
    }

        @Test
    void testTimeOutNotReached_JustStarted() {
        Instant startTime = Instant.now();
        long timeoutSeconds = 5; // 5 seconds timeout

        boolean result = MiscUtil.timeOutReached(startTime, timeoutSeconds);

        assertFalse(result, "Timeout should not have been reached immediately after start");
    }

    @Test
    void testTimeOutReached_AfterDelay() throws InterruptedException {
        Instant startTime = Instant.now().minusSeconds(6);
        long timeoutSeconds = 5;

        boolean result = MiscUtil.timeOutReached(startTime, timeoutSeconds);

        assertTrue(result, "Timeout should be reached after waiting longer than the timeout");
    }

    @Test
    void testTimeOutExactlyReached() {
        Instant startTime = Instant.now().minusSeconds(10);
        long timeoutSeconds = 10;

        boolean result = MiscUtil.timeOutReached(startTime, timeoutSeconds);

        assertTrue(result, "Timeout should be considered reached at the exact timeout boundary");
    }

    @Test
    void testNegativeTimeoutAlwaysTrue() {
        Instant startTime = Instant.now();
        long timeoutSeconds = -1;

        boolean result = MiscUtil.timeOutReached(startTime, timeoutSeconds);

        assertTrue(result, "Negative timeout should always result in true");
    }

    @Test
    void testZeroTimeout() {
        Instant startTime = Instant.now().minusMillis(1);
        long timeoutSeconds = 0;

        boolean result = MiscUtil.timeOutReached(startTime, timeoutSeconds);

        assertTrue(result, "Zero timeout should trigger immediately");
    }

}
