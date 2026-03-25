package com.netcracker.common.models;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SuspendRangeTest {

    @Test
    void durations() {
        var r1 = new SuspendRange();
        assertEquals(0, r1.size());
        assertEquals(0, r1.getSuspendDuration(50, 250));

        r1.add(100L, 10);
        r1.add(230L, 50);
        assertEquals(2, r1.size());
        assertEquals(60, r1.getSuspendDuration(50, 250));
        assertEquals(10, r1.getSuspendDuration(70, 120));
        assertEquals(10, r1.getSuspendDuration(90, 100));
        assertEquals(5, r1.getSuspendDuration(95, 100));
        assertEquals(5, r1.getSuspendDuration(60, 95));

        var r2 = new SuspendRange();
        r2.addAll(r1);
        assertEquals(5, r2.getSuspendDuration(60, 95));

        var r3 = new SuspendRange();
        r3.addAll(r1);
        r3.addAll(r2);
        assertEquals(5, r3.getSuspendDuration(60, 95));

    }

}