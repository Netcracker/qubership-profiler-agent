package com.netcracker.persistence.utils;

import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.regex.Pattern;

import com.netcracker.profiler.model.DurationUnit;

public interface Constants {
    String timeoutEnv = System.getenv("CDT_GATEWAY_TIMEOUT");
    long REQUEST_TIMEOUT = timeoutEnv != null ? Long.parseLong(timeoutEnv) : 30; // default 30 seconds
    String INVERTED_INDEX_PARAMS = System.getenv().getOrDefault("INVERTED_INDEX_PARAMS", "");
    String INVERTED_INDEX_GRANULARITY = System.getenv().getOrDefault("INVERTED_INDEX_GRANULARITY", "1h");
    DurationUnit DEFAULT_INVERTED_INDEX_GRANULARITY = new DurationUnit(1, ChronoUnit.HOURS);
    Map<String, ChronoUnit> INVERTED_INDEX_GRANULARITY_UNITS = Map.of(
            "h", ChronoUnit.HOURS,
            "m", ChronoUnit.MINUTES);
    String INVERTED_INDEX_LIFETIME = System.getenv().getOrDefault("INVERTED_INDEX_LIFETIME", "14d");
    DurationUnit DEFAULT_INVERTED_INDEX_LIFETIME = new DurationUnit(14, ChronoUnit.DAYS);
    Map<String, ChronoUnit> INVERTED_INDEX_LIFETIME_UNITS = Map.of(
            "h", ChronoUnit.HOURS,
            "d", ChronoUnit.DAYS);
    /*
     * Regex exp to parse query parameters for eg.
     * +param1 "Val1" AND -param2 "Val2" OR NOT +param3 "Val3"
     */
    Pattern QUERY_REGEX = Pattern.compile("[+-]?\"[^\"]*?\"|\\S+");
    String DEFAULT_S3_FILES_LIMIT = "20";
}