package com.netcracker.profiler.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

import org.apache.commons.lang.StringUtils;

import static com.netcracker.persistence.utils.Constants.QUERY_REGEX;

/**
 * Represents a parsed query filter, separating included and excluded parameter-value conditions.
 * Parses queries with +key value, -key value, and logical operators like AND, OR, NOT.
 *
 * Example input: "+service login -traceId abc OR NOT +spanId xyz"
 * Result:
 *   included: {service=[login], spanId=[xyz]}
 *   excluded: {traceId=[abc]}
 */
public class QueryFilter {

    // Included filters: e.g., +key value
    Map<String, List<String>> included = new HashMap<>();

    // Excluded filters: e.g., -key value or NOT +key value
    Map<String, List<String>> excluded = new HashMap<>();

    /**
     * Returns the map of included filters.
     * @return Map of included keys and their values.
     */
    public Map<String, List<String>> get_included() {
        return included;
    }

    /**
     * Returns the map of excluded filters.
     * @return Map of excluded keys and their values.
     */
    public Map<String, List<String>> get_excluded() {
        return excluded;
    }

    /**
     * Flushes the current key and value list into either the included or excluded filter map.
     * Used internally while parsing.
     *
     * @param condition QueryFilter object to update.
     * @param key       The current key being parsed.
     * @param values    List of values to associate with the key.
     * @param include   True if key-values should go into 'included', false for 'excluded'.
     */
    private static void flushToCondition(QueryFilter condition, String key, List<String> values, boolean include) {
        if (key == null || values.isEmpty())
            return;
        Map<String, List<String>> target = include ? condition.get_included() : condition.get_excluded();
        target.computeIfAbsent(key, k -> new ArrayList<>()).addAll(values);
    }

    /**
     * Parses a query filter string into a QueryFilter object with included and excluded maps.
     *
     * Supported syntax:
     * - +key value (included)
     * - -key value (excluded)
     * - NOT +key value (negated, becomes excluded)
     * - AND, OR are supported as delimiters but not structurally stored
     *
     * @param queryFilter Raw query filter string.
     * @return Parsed QueryFilter object.
     *
     * Example:
     *   Input: "+service login AND -traceId abc OR NOT +spanId xyz"
     *   Output:
     *     included = {service=[login]}
     *     excluded = {traceId=[abc], spanId=[xyz]}
     */
    public static QueryFilter parseQueryFilter(String queryFilter) {
        QueryFilter condition = new QueryFilter();
        if (StringUtils.isEmpty(queryFilter))
            return condition;

        Matcher matcher = QUERY_REGEX.matcher(queryFilter);
        String currentKey = null;
        boolean include = true;
        boolean nextIsNegated = false;
        List<String> values = new ArrayList<>();

        while (matcher.find()) {
            String token = matcher.group();
            if (token.equalsIgnoreCase("AND") || token.equalsIgnoreCase("OR")) {
                flushToCondition(condition, currentKey, values, include);
                currentKey = null;
                values.clear();
                continue;
            } else if (token.equalsIgnoreCase("NOT")) {
                nextIsNegated = true;
                continue;
            }

            if (token.startsWith("+") || token.startsWith("-")) {
                flushToCondition(condition, currentKey, values, include);
                currentKey = token.substring(1);
                include = token.startsWith("+");
                if (nextIsNegated) {
                    include = !include;
                    nextIsNegated = false;
                }
                values.clear();
            } else if (currentKey != null) {
                values.add(StringUtils.strip(token, "\""));
            }
        }

        flushToCondition(condition, currentKey, values, include);
        return condition;
    }

    /**
     * Returns a string representation of the filter maps.
     * @return Human-readable representation.
     */
    @Override
    public String toString() {
        return "QueryFilter{" +
                "included=" + included +
                ", excluded=" + excluded +
                '}';
    }
}
