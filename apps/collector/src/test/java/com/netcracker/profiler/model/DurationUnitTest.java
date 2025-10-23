package com.netcracker.profiler.model;

import org.junit.jupiter.api.Test;

import java.time.temporal.ChronoUnit;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DurationUnitTest {

    @Test
    void testParseDurationUnit_validDays() {
        DurationUnit result = DurationUnit.parseDurationUnit("5d",
                Map.of("d", ChronoUnit.DAYS, "h", ChronoUnit.HOURS),
                new DurationUnit(7, ChronoUnit.DAYS));

        assertEquals(5, result.amount);
        assertEquals(ChronoUnit.DAYS, result.unit);
    }

    @Test
    void testParseDurationUnit_validHours() {
        DurationUnit result = DurationUnit.parseDurationUnit("12h",
                Map.of("d", ChronoUnit.DAYS, "h", ChronoUnit.HOURS),
                new DurationUnit(1, ChronoUnit.DAYS));

        assertEquals(12, result.amount);
        assertEquals(ChronoUnit.HOURS, result.unit);
    }

    @Test
    void testParseDurationUnit_withSpacesAndUpperCase() {
        DurationUnit result = DurationUnit.parseDurationUnit(" 10H ",
                Map.of("h", ChronoUnit.HOURS),
                new DurationUnit(0, ChronoUnit.MINUTES));

        assertEquals(10, result.amount);
        assertEquals(ChronoUnit.HOURS, result.unit);
    }

    @Test
    void testParseDurationUnit_nullOrBlank_returnsDefault() {
        DurationUnit defaultUnit = new DurationUnit(3, ChronoUnit.DAYS);

        assertEquals(defaultUnit.amount,
                DurationUnit.parseDurationUnit(null, Map.of(), defaultUnit).amount);
        assertEquals(defaultUnit.unit,
                DurationUnit.parseDurationUnit("   ", Map.of(), defaultUnit).unit);
    }

    @Test
    void testParseDurationUnit_invalidSuffix_throwsException() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> DurationUnit.parseDurationUnit("5x",
                        Map.of("d", ChronoUnit.DAYS, "h", ChronoUnit.HOURS),
                        new DurationUnit(0, ChronoUnit.MINUTES)));

        assertEquals("Invalid value: 5x", ex.getMessage());
    }

    @Test
    void testParseDurationUnit_nonNumericAmount_throwsException() {
        assertThrows(NumberFormatException.class, () -> DurationUnit.parseDurationUnit("xd",
                Map.of("d", ChronoUnit.DAYS),
                new DurationUnit(1, ChronoUnit.DAYS)));
    }
}
