package com.netcracker.common.models.meta.dict;

import org.apache.commons.lang.StringUtils;

import java.util.*;

import static com.netcracker.common.Consts.KNOWN_IDLE_URLS;

public record CallParameters(Map<String, List<String>> params) {

    public int size() {
        return params.size();
    }

    public Set<String> names() {
        return params.keySet();
    }

    public List<String> values(String name) {
        return params.get(name);
    }

    public Map<String, List<String>> asMap() {
        return params;
    }

    public void put(String key, List<String> values) {
        params.put(key, values);
    }

    public boolean isSystem() {
        for (var param: params.entrySet()) {
            if (isSystem(param.getKey(), param.getValue())) {
                return true;
            }
        }
        return false;
    }

    public static boolean isSystem(String name, List<String> values) {
        if (Parameter.isIdle(name)) {
            return true;
        }
        return isIdleUrl(name, values);
    }

    public static boolean isIdleUrl(String name, List<String> values) {
        if (Parameter.isWebUrl(name) && !isEmpty(name, values)) {
            for (var knownUrl : KNOWN_IDLE_URLS) {
                if (values.get(0).endsWith(knownUrl)) {
                    return true;
                }
            }
        }
        return false;
    }

    public static boolean isEmpty(String name, List<String> values) {
        return isInvalid(name, values) || values.isEmpty();
    }

    public static boolean isInvalid(String name, List<String> values) {  // could not resolve tag
        return StringUtils.isEmpty(name) || values == null;
    }

}
