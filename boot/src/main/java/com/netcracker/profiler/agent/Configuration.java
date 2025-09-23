package com.netcracker.profiler.agent;

import java.util.Map;

public interface Configuration {
    public String getStoreTransformedClassesPath();

    /**
     * Used to return map of parameter name to parameter type.
     * Currently method replaced with Configuration_01#getParametersInfo
     * @return empty map
     * @see Configuration_01#getParametersInfo
     */
    @Deprecated
    public Map<String, Integer> getParamTypes();

    /**
     * Updates parameter info with new type
     * @param param name of parameter to update
     * @param type new type of the parameter
     * @see Configuration_01#getParameterInfo
     */
    @Deprecated
    public void setParamType(String param, int type);

    public EnhancementRegistry getEnhancementRegistry();
}
