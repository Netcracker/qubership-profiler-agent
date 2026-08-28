package com.netcracker.profiler.agent;

import java.util.Map;

public interface EnhancerRegistryPlugin {
    void addEnhancerPlugin(String name, Object/*EnhancerPlugin*/ plugin);

    Object/*EnhancerPlugin*/ getEnhancerPlugin(String name);

    Map<String, Object/*EnhancerPlugin*/> getEnhancersMap();
}
