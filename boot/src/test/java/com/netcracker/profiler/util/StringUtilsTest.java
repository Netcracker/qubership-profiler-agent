package com.netcracker.profiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.netcracker.profiler.agent.StringUtils;

import org.junit.jupiter.api.Test;

public class StringUtilsTest {
    @Test
    public void arrayToStringTest() {
        assertEquals("[1, 2, 3]", StringUtils.arrayToString(new StringBuilder(), new Integer[]{1, 2, 3}).toString());
        assertEquals("[1, 2 (3 times), (4):3]", StringUtils.arrayToString(new StringBuilder(), new Integer[]{1,2,2,2,3}).toString());
        assertEquals("[5 (3 times)]", StringUtils.arrayToString(new StringBuilder(), new Integer[]{5,5,5}).toString());
        assertEquals("[6, 5 (3 times)]", StringUtils.arrayToString(new StringBuilder(), new Integer[]{6, 5,5,5}).toString());
    }
}
