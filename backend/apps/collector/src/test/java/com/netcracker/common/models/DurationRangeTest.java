package com.netcracker.common.models;

import com.netcracker.utils.UnitTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@UnitTest
class DurationRangeTest {

    @Test
    void inRange() {
        assertFalse(DurationRange.ofSeconds(1, 5).inRange(500));
        assertTrue(DurationRange.ofSeconds(1, 5).inRange(1000));
        assertTrue(DurationRange.ofSeconds(1, 5).inRange(1500));
        assertTrue(DurationRange.ofSeconds(1, 5).inRange(4500));
        assertTrue(DurationRange.ofSeconds(1, 5).inRange(4999));
        assertTrue(DurationRange.ofSeconds(1, 5).inRange(5000));
        assertFalse(DurationRange.ofSeconds(1, 5).inRange(5001));
    }

    @Test
    void hash() {
        assertEquals("PT1S_PT5S", DurationRange.ofSeconds(1, 5).hash());
        assertEquals("[PT1S-PT5S]", DurationRange.ofSeconds(1, 5).toString());
        assertEquals("PT1.3S_PT4.5S", DurationRange.ofMillis(1300, 4500).hash());
    }

}