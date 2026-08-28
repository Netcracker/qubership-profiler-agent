package com.netcracker.profiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class VersionUtilsTest {
    @Test
    public void testSimpleVersion() {
        assertEquals("001.023b002.001", VersionUtils.naturalOrder("1.23b2.1", 3));
    }

    @Test
    public void testNoNumeric() {
        assertEquals("test", VersionUtils.naturalOrder("test", 5));
    }

    @Test
    public void testEmpty() {
        assertEquals("", VersionUtils.naturalOrder("", 3));
    }

    @Test
    public void testJustNumber() {
        assertEquals("123", VersionUtils.naturalOrder("123", 3));
    }

    @Test
    public void testSmallWidth() {
        assertEquals("1.23b2.1", VersionUtils.naturalOrder("1.23b2.1", 1));
        assertEquals("01.23b02.01", VersionUtils.naturalOrder("1.23b2.1", 2));
    }

    @Test
    public void testOldFormat() {
        assertEquals("03.05-00", VersionUtils.naturalOrder("v3.5-0", 2));
    }
}
