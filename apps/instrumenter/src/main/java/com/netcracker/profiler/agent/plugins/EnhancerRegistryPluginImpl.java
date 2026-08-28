package com.netcracker.profiler.agent.plugins;

import com.netcracker.profiler.agent.Bootstrap;
import com.netcracker.profiler.agent.EnhancerRegistryPlugin;
import com.netcracker.profiler.instrument.enhancement.EnhancerPlugin;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class EnhancerRegistryPluginImpl implements EnhancerRegistryPlugin {
    private final Map<String, EnhancerPlugin> enhancers = new HashMap<String, EnhancerPlugin>();

    public EnhancerRegistryPluginImpl() {
        Bootstrap.registerPlugin(EnhancerRegistryPlugin.class, this);
    }

    public void addEnhancerPlugin(String name, Object plugin) {
        enhancers.put(name, (EnhancerPlugin) plugin);
    }

    public Object getEnhancerPlugin(String name) {
        return enhancers.get(name);
    }

    public Map<String, Object> getEnhancersMap() {
        return Collections.unmodifiableMap((Map) enhancers);
    }
}
