package org.qubership.profiler.util;

import org.qubership.profiler.agent.StringUtils;

import org.testng.Assert;
import org.testng.annotations.Test;

public class StringUtilsTest {
    @Test
    public void arrayToStringTest() {
        Assert.assertEquals("[1, 2, 3]", StringUtils.arrayToString(new StringBuilder(), new Integer[]{1, 2, 3}).toString());
        Assert.assertEquals("[1, 2 (3 times), (4):3]", StringUtils.arrayToString(new StringBuilder(), new Integer[]{1,2,2,2,3}).toString());
        Assert.assertEquals("[5 (3 times)]", StringUtils.arrayToString(new StringBuilder(), new Integer[]{5,5,5}).toString());
        Assert.assertEquals("[6, 5 (3 times)]", StringUtils.arrayToString(new StringBuilder(), new Integer[]{6, 5,5,5}).toString());
    }
}
