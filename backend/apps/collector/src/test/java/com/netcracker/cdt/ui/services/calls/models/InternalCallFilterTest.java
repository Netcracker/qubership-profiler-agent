package com.netcracker.cdt.ui.services.calls.models;

import com.netcracker.cdt.ui.services.calls.search.InternalCallFilter;
import com.netcracker.common.models.DurationRange;
import com.netcracker.utils.UnitTest;
import com.netcracker.profiler.model.Call;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@UnitTest
class InternalCallFilterTest {

    @Test
    void filter() {
        assertFalse(new InternalCallFilter(DurationRange.ofSeconds(1, 5)).filter(call(0)));
        assertTrue(new InternalCallFilter(DurationRange.ofSeconds(1, 5)).filter(call(1000)));
        assertTrue(new InternalCallFilter(DurationRange.ofSeconds(1, 5)).filter(call(4999)));
        assertTrue(new InternalCallFilter(DurationRange.ofSeconds(1, 5)).filter(call(5000)));
        assertFalse(new InternalCallFilter(DurationRange.ofSeconds(1, 5)).filter(call(7000)));
    }

    private static Call call(int durationMs) {
        var c = new Call();
        c.duration = durationMs;
        return c;
    }
}