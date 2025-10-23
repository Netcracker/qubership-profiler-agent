package com.netcracker.profiler.model;

import java.time.temporal.ChronoUnit;
import java.util.Map;

/**
 * Represents a duration with a numeric amount and a time unit (ChronoUnit).
 * Useful for interpreting string-based duration configurations like "30m" or "2h".
 */
public class DurationUnit {

    public long amount;
    public ChronoUnit unit;

    /**
     * Constructs a DurationUnit with the specified amount and unit.
     *
     * Example:
     *   new DurationUnit(15, ChronoUnit.MINUTES)
     *
     * @param amount Numeric duration value
     * @param unit   Time unit of the duration (ChronoUnit)
     */
    public DurationUnit(long amount, ChronoUnit unit) {
        this.amount = amount;
        this.unit = unit;
    }

    /**
     * Parses a string like "30m" or "2h" into a DurationUnit using the provided suffix-to-unit map.
     * If the input is null or blank, returns the given defaultDurationUnit.
     *
     * Example:
     *   parseDurationUnit("5d", Map.of("d", DAYS), new DurationUnit(1, DAYS)) => DurationUnit(5, DAYS)
     *
     * @param durationUnitStr      Input string like "30m" or "1h"
     * @param unitMap              Map of suffixes (e.g., "h", "m") to ChronoUnits
     * @param defaultDurationUnit  Fallback DurationUnit to return on empty input
     * @return Parsed DurationUnit
     * @throws IllegalArgumentException if format is invalid or suffix not found in unitMap
     */
    public static DurationUnit parseDurationUnit(String durationUnitStr,
                                                 Map<String, ChronoUnit> unitMap,
                                                 DurationUnit defaultDurationUnit) {
        if (durationUnitStr == null || durationUnitStr.isBlank()) {
            return defaultDurationUnit;
        }

        durationUnitStr = durationUnitStr.trim().toLowerCase();

        for (Map.Entry<String, ChronoUnit> entry : unitMap.entrySet()) {
            String suffix = entry.getKey();
            if (durationUnitStr.endsWith(suffix)) {
                long amount = Long.parseLong(durationUnitStr.substring(0, durationUnitStr.length() - suffix.length()));
                return new DurationUnit(amount, entry.getValue());
            }
        }

        throw new IllegalArgumentException("Invalid value: " + durationUnitStr);
    }
}
