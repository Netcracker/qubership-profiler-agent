package com.netcracker.common.search.filter;

import java.util.*;

/**
 * Keep parsed value from UI filter (see FilterParser.java).
 * <p>
 * 1. all keywords from `mandatory` list should be present
 * 2. none of keywords from `excluded` list should be present
 * 3. at least one from `included` list should be present
 * <p>
 * Check strings by `contains` function, not `equals`
 *
 * @param hideSystem
 * @param included
 * @param excluded
 * @param mandatory
 */
public record FilterCondition(boolean hideSystem, boolean hasMandatoryParams, List<FilterValue> included,
                              List<FilterValue> excluded,
                              List<FilterValue> mandatory) {

    public boolean isEmpty() {
        return included.isEmpty() && excluded.isEmpty() && mandatory.isEmpty();
    }

    /**
     * @param strictMode enables special mode when all of mandatory fields must present + at least one of optional
     */
    public Matcher start(boolean strictMode) {
        return new Matcher(strictMode);
    }

    public FilterCondition copyWithPopulatedIds(Map<String, Integer> paramToIdMapping) {
        if (paramToIdMapping == null) {
            return this;
        }
        var inc = new ArrayList<FilterValue>();
        for (var c : this.included()) {
            if (paramToIdMapping.containsKey(c.paramName())) {
                inc.add(c.withParamId(paramToIdMapping.get(c.paramName())));
            } else {
                inc.add(c);
            }
        }

        var ex = new ArrayList<FilterValue>();
        for (var c : this.excluded()) {
            if (paramToIdMapping.containsKey(c.paramName())) {
                ex.add(c.withParamId(paramToIdMapping.get(c.paramName())));
            } else {
                ex.add(c);
            }
        }

        var man = new ArrayList<FilterValue>();
        for (var c : this.mandatory()) {
            if (paramToIdMapping.containsKey(c.paramName())) {
                man.add(c.withParamId(paramToIdMapping.get(c.paramName())));
            } else {
                man.add(c);
            }
        }
        return new FilterCondition(hideSystem, hasMandatoryParams, inc, ex, man);
    }

    public final class Matcher {
        boolean hasIncluded;
        Map<FilterValue, Boolean> foundMandatory;
        boolean foundIncluded;
        boolean foundExcluded;

        /**
         * @param strictMode enables special mode when all of mandatory fields must present + at least one of optional
         */
        public Matcher(boolean strictMode) {
            // last resort for non-strict mode:
            //  for general keywords will filter later, during UiCallRecordFilter

            this.foundExcluded = false;

            this.foundIncluded = false;
            this.hasIncluded = false;
            for (var p : included) {
                if (strictMode || p.hasParameter()) {
                    hasIncluded = true;
                    break;
                }
            }

            this.foundMandatory = new HashMap<>();
            for (var p : mandatory) {
                if (strictMode || p.hasParameter()) {
                    foundMandatory.put(p, false);
                }
            }
        }

        /**
         * To check parameter values (before we load parameter names by their tagIds)
         *
         * @return {@code true} if already obvious it won't match; {@code false} otherwise
         */

        public <T> boolean addParameterValuesById(int parameterId, List<T> values) {
            // non-strict mode
            for (var p : excluded) {
                if (!p.hasParameter()) continue;
                if (p.check(parameterId, values)) {
                    foundExcluded = true;
                    return true;
                }
            }
            for (var p : mandatory) {
                if (!p.hasParameter()) continue;
                if (p.check(parameterId, values)) {
                    foundMandatory.put(p, true);
                }
            }
            for (var p : included) {
                if (!p.hasParameter()) continue;
                if (p.check(parameterId, values)) {
                    foundIncluded = true;
                    break;
                }
            }
            return false;
        }

        /**
         * To check parameter values (when we already load parameter names by their tagIds)
         *
         * @return {@code true} if already obvious it won't match; {@code false} otherwise
         */
        public <T> boolean addParameterValuesByName(String parameterName, List<T> values) {
            // strict mode
            for (var excluded : excluded) {
                if (excluded.check(parameterName, values)) {
                    foundExcluded = true;
                    return true;
                }
            }
            for (var mandatory : mandatory) {
                if (mandatory.check(parameterName, values)) {
                    foundMandatory.put(mandatory, true);
                }
            }
            for (var included : included) {
                if (included.check(parameterName, values)) {
                    foundIncluded = true;
                    break;
                }
            }
            return false;
        }

        /**
         * Check against general strings (like method names), not parameter values
         *
         * @return {@code true} if already obvious it won't match; {@code false} otherwise
         */
        public boolean addGeneralString(String s) {
            // strict mode
            for (var excluded : excluded) {
                if (excluded.check(s)) {
                    foundExcluded = true;
                    return true;
                }
            }
            for (var mandatory : mandatory) {
                if (mandatory.check(s)) {
                    foundMandatory.put(mandatory, true);
                }
            }
            for (var included : included) {
                if (included.check(s)) {
                    foundIncluded = true;
                    break;
                }
            }
            return false;
        }

        public boolean matches() {
            if (foundExcluded) {
                return false;
            }
            if (hasIncluded && !foundIncluded) {
                return false;
            }
            for (var b : foundMandatory.values()) {
                if (!b) return false;
            }
            return true;
        }

    }

}
