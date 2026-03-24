package com.netcracker.common.search.filter;

import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

/**
 * Parse query from UI to interval structure `FilterCondition` to further filtering.
 * <p>
 * 1. Use + before a word for mandatory, - to exclude, or quotes to 'exact phrase' filtering
 * 2. Use $param=value for filtering of parameter value
 * 3. Keywords without modifiers (+,-) works as a `SHOULD` condition - at least one of these words should be found
 * 4. System looks for keywords by `contains` function, not `equals`
 * <p>
 * Examples:
 * `+clust1 sysadm administrator`      - lists all (sysadm OR administrator requests) made to clust1 node.
 * `'test page' -cust2`                - lists request matching phrase 'test page' except the requests to clust2.
 * `+clust1 -jsp sysadm administrator` - lists (sysadm or administrator) requests that match clust2 except jsp calls.
 * `+$node.name=clust1 -$web.url=jsp $nc.user=sysadm $nc.user=administrator`
 *                                     - the same as above search, but explicitly sets parameters for searching.
 */
public class FilterParser {
    private static final Pattern FILTER_QUERY_REGEX = Pattern.compile("([+-]?((\"[^\"]*?\")|(\'[^\']*?\')|(\\`[^\\`]*?\\`)))|\\S+");
    private static final Pattern PARAMETER_REGEX = Pattern.compile("\\$(\\S+)=(\\S+)");

    public static FilterCondition parse(String filterString) {
        if (StringUtils.isBlank(filterString)) {
            var empty = Collections.<FilterValue>emptyList();
            return new FilterCondition(false, false, empty, empty, empty);
        }

        final var mandatory = new ArrayList<FilterValue>();
        final var included = new ArrayList<FilterValue>();
        final var excluded = new ArrayList<FilterValue>();

        boolean hideSystem = false; // TODO parse from quary

        FILTER_QUERY_REGEX.matcher(filterString).results().forEach(mr -> {
            String expr = mr.group(0);
            if (StringUtils.isBlank(expr)) {
                return;
            }
            char c = expr.charAt(0);
            var toAddTo = switch (c) {
                case '+' -> mandatory;
                case '-' -> excluded;
                default -> included;
            };

            if (c == '+' || c == '-') {
                expr = expr.substring(1);
            }

            expr = stripQuote(expr);

            if (!StringUtils.isBlank(expr)) {
                var found = new AtomicBoolean(false);
                PARAMETER_REGEX.matcher(expr).results().forEach(pr -> {
                    if (pr.groupCount() == 2) {
                        found.set(true);
                        var param = stripQuote(pr.group(1));
                        var val = stripQuote(pr.group(2));
                        toAddTo.add(FilterValue.from(param, val));
                    }
                });
                if (!found.get()) {
                    toAddTo.add(FilterValue.from(expr));
                }
            }
        });

        boolean hasMandatoryParams = false;
        for (var v: mandatory) {
            if (v.hasParameter()) {
                hasMandatoryParams = true;
            }
        }
        return new FilterCondition(hideSystem, hasMandatoryParams, included, excluded, mandatory);
    }

    public static String stripQuote(String expr) {
        if (expr.length() > 2) {
            if (expr.charAt(0) == '"' && expr.charAt(expr.length() - 1) == '"') {
                expr = expr.substring(1, expr.length() - 1);
            }
            if (expr.charAt(0) == '\'' && expr.charAt(expr.length() - 1) == '\'') {
                expr = expr.substring(1, expr.length() - 1);
            }
            if (expr.charAt(0) == '`' && expr.charAt(expr.length() - 1) == '`') {
                expr = expr.substring(1, expr.length() - 1);
            }
        }
        return expr;
    }
}
