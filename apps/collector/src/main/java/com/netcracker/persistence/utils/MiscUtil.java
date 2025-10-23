package com.netcracker.persistence.utils;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.parquet.example.data.Group;

/**
 * Utility class for common string normalization, list formatting,
 * and Parquet group parsing.
 */
public class MiscUtil {

    /**
     * Normalizes a parameter by converting to lowercase and removing all
     * non-alphanumeric characters.
     * Throws exception if result is empty or starts with a digit.
     *
     * Example:
     * normalizeParam("Request.Id#123") => "requestid123"
     *
     * @param param Input string to normalize.
     * @return Normalized string.
     * @throws IllegalArgumentException if input is null, empty, or starts with a
     *                                  digit after normalization.
     */
    public static String normalizeParam(String param) throws IllegalArgumentException {
        if (param == null) {
            throw new IllegalArgumentException("Input parameter is null");
        }

        param = param.toLowerCase();

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < param.length(); i++) {
            char c = param.charAt(i);
            if ((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')) {
                sb.append(c);
            }
        }

        String normalized = sb.toString();

        if (normalized.isEmpty()) {
            throw new IllegalArgumentException("Parameter normalization failed: result is empty after stripping");
        }

        if (Character.isDigit(normalized.charAt(0))) {
            throw new IllegalArgumentException(
                    String.format("Parameter normalization failed: result starts with a digit ('%c')",
                            normalized.charAt(0)));
        }

        return normalized;
    }

    /**
     * Normalizes a comma-separated list of parameters using normalizeParam.
     *
     * Example:
     * normalizeParamList("request.id, job-name") => "requestid,jobname"
     *
     * @param paramList Comma-separated list string.
     * @return Comma-separated normalized string.
     */
    public static String normalizeParamList(String paramList) {
        if (paramList == null || paramList.isBlank())
            return "";

        return Arrays.stream(paramList.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(MiscUtil::normalizeParam)
                .collect(Collectors.joining(","));
    }

    /**
     * Converts a Parquet Group object into a map of key to list of values.
     * Expects group schema with nested fields "key_value" and "list".
     *
     * @param input Parquet Group object.
     * @return Map with key to list of values.
     */
    public static Map<String, List<String>> readGroupToHashMap(Group input) {
        Map<String, List<String>> resultMap = new HashMap<>();
        Group tempGroup = input;
        String key = null;
        List<String> values = new ArrayList<>();
        while (tempGroup != null) {
            var tempType = tempGroup.getType();
            for (int i = 0; i < tempType.getFieldCount(); i++) {
                if (tempType.getFields().get(i).isPrimitive()) {
                    String type = tempType.getName();
                    if (type.equals("key_value")) {
                        if (tempGroup.getFieldRepetitionCount(i) > 0) {
                            key = tempGroup.getValueToString(i, 0);
                            values = new ArrayList<>();
                        }
                    } else if (type.equals("list")) {
                        if (tempGroup.getFieldRepetitionCount(i) > 0) {
                            values.add(tempGroup.getValueToString(i, 0));
                            if (key != null) {
                                resultMap.put(key, values);
                            }
                        }
                    }
                    if (tempType.getFieldCount() == 1) {
                        tempGroup = null;
                    }
                } else {
                    if (tempGroup.getFieldRepetitionCount(i) > 0) {
                        tempGroup = tempGroup.getGroup(i, 0);
                    } else {
                        tempGroup = null;
                    }
                }
            }
        }
        return resultMap;
    }

    /**
     * Checks whether rootMap contains at least one matching value from subMap for
     * any common key.
     *
     * Example:
     * rootMap = { "key1": ["a", "b"] }
     * subMap = { "key1": ["b", "c"] }
     * returns true
     *
     * @param rootMap The larger map to check in.
     * @param subMap  The map to check from.
     * @return true if any matching value found, else false.
     */
    public static boolean containsValuesInMap(Map<String, List<String>> rootMap, Map<String, List<String>> subMap) {
        for (String key : rootMap.keySet()) {
            List<String> subMapValues = subMap.get(key);
            List<String> rootMapValues = rootMap.get(key);

            if (rootMapValues == null || subMapValues == null) {
                continue;
            }

            for (String value : subMapValues) {
                if (rootMapValues.contains(value)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Formats a list of strings into a single-quoted comma-separated string.
     *
     * Example:
     * getQuotedStringOfList(List.of("a", "b")) => "'a','b'"
     *
     * @param list List of strings.
     * @return Formatted string.
     */
    public static String getQuotedStringOfList(List<String> list) {
        return list.stream()
                .map(namespace -> "'" + namespace + "'")
                .collect(Collectors.joining(","));
    }

    /**
     * Checks whether the specified timeout duration has been exceeded
     * since the provided start time.
     *
     * @param startTime the Instant when the process started
     * @param timeOut   the timeout duration in seconds
     * @return true if the current time is greater than startTime plus timeout,
     *         false otherwise
     */
    public static boolean timeOutReached(Instant startTime, long timeOut) {
        Duration timeOutDur = Duration.ofSeconds(timeOut);
        return Duration.between(startTime, Instant.now()).compareTo(timeOutDur) > 0;
    }
}
