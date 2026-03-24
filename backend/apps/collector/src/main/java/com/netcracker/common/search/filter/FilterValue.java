package com.netcracker.common.search.filter;

import java.util.List;
import java.util.Objects;

public record FilterValue(Integer paramId, String paramName, String value) {

    public static FilterValue from(String value) {
        // keyword without parameter (look for all occurrences)
        return new FilterValue(null, "", value.toLowerCase());
    }

    public static FilterValue from(String parameter, String value) {
        // keyword for special parameter
        return new FilterValue(null, parameter, value.toLowerCase());
    }

    public FilterValue withParamId(Integer paramId) {
        return new FilterValue(paramId, paramName, value);
    }

    public boolean hasParameter() {
        return !paramName.isEmpty() || paramId != null;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (other == null || getClass() != other.getClass()) return false;
        var o = (FilterValue) other;
        return Objects.equals(value, o.value) && Objects.equals(paramName, o.paramName);
    }

    @Override
    public int hashCode() {
        return paramName.hashCode();
    }

    @Override
    public String toString() {
        String id = "";
        if (paramId != null) {
            id = paramId + ":";
        }
        if (!paramName.isEmpty()) {
            id += paramName + "=";
        }
        return "[%s='%s']".formatted(id, value);
    }

    // check against general strings (like method names), not parameter values
    public boolean check(String actual) {
        if (hasParameter()) {
            return false; // check only for general keywords
        }
        return actual != null && actual.toLowerCase().contains(value);
    }

    // check against parameter values (when we already load parameter names by their tagIds)
    public <T> boolean check(String parameterName, List<T> actual) {
        if (actual == null) {
            return false;
        }
        // check against general keywords or specified parameter:
        if (!paramName.isEmpty() && !parameterName.equals(paramName)) {
            // has parameter, but it doesn't match
            return false;
        }
        for (var s : actual) {
            if (s != null && s.toString().toLowerCase().contains(value)) {
                return true;
            }
        }
        if (paramName.isEmpty() && parameterName.toLowerCase().contains(value)) {
            // general keywords matches parameterName
            return true;
        }
        return false;
    }

    // check against parameter values (before we load parameter names by their tagIds)
    public <T> boolean check(int parameterId, List<T> actual) {
        if (actual == null) {
            return false;
        }
        // check against general keywords or specified parameter:
        if (paramId != null && parameterId != paramId) {
            // has parameter, but it doesn't match
            return false;
        }
        for (var s : actual) {
            if (s != null && s.toString().toLowerCase().contains(value)) {
                return true;
            }
        }
        return false;
    }


}
