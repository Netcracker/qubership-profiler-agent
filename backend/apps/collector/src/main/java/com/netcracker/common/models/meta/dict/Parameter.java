package com.netcracker.common.models.meta.dict;

import org.apache.commons.lang.StringUtils;

import static com.netcracker.common.Consts.*;

public record Parameter(
        String paramName,
//        boolean big, boolean deduplicate, // ? sql, xpath, xml
        boolean index, boolean list, int order,
        String signatureFunction
) implements Comparable<Parameter> {

    public static final Parameter THREAD_NAME = Parameter.of(JAVA_THREAD);

    public static Parameter of(String name, boolean idx, boolean list, int order, String signature) {
        return new Parameter(name, idx, list, order, signature);
    }

    private static Parameter of(String name) {
        return new Parameter(name, false, false, 0, name);
    }

    public String name() {
        return paramName;
    }

    public boolean isInvalid() {  // could not resolve tag
        return StringUtils.isEmpty(name());
    }

    public boolean isIdle() {
        return isIdle(name());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Parameter parameter = (Parameter) o;

        return name().equals(parameter.name());
    }

    @Override
    public int hashCode() {
        return name().hashCode();
    }

    @Override
    public String toString() {
        return String.format("[%s]", name());
    }

    @Override
    public int compareTo(Parameter o) {
        var r = order - o.order;
        if (r == 0) {
            r = name().compareTo(o.name());
        }
        return r;
    }

    public static boolean isIdle(String name) {
        return CALLS_IDLE.equals(name) || ASYNC_ABSORBED.equals(name);
    }

    public static boolean isWebUrl(String name) {
        return WEB_URL.equals(name);
    }

}

