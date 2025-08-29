package org.qubership.profiler.instrument.enhancement;

import org.qubership.profiler.agent.Bootstrap;
import org.qubership.profiler.agent.Configuration_01;
import org.qubership.profiler.agent.EnhancementRegistry;
import org.qubership.profiler.agent.EnhancerRegistryPlugin;
import org.qubership.profiler.configuration.ConfigStackElement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import java.util.Map;

public class EnhancerPlugin {
    public static final Logger log = LoggerFactory.getLogger(EnhancerPlugin.class);

    public final Map<String, ClassEnhancer> enhancersCollection;
    private ConfigStackElement stack;

    public EnhancerPlugin() {
        String name = getClass().getName() + "Enhancers";
        log.debug("Trying to find enhancer {}", name);

        Map<String, ClassEnhancer> enhancersCollection = null;
        try {
            final Class<?> enhancerClass = Class.forName(name, true, getClass().getClassLoader());
            enhancersCollection = (Map<String, ClassEnhancer>) enhancerClass.newInstance();
        } catch (ClassNotFoundException e) {
            log.warn("Unable to find enhancers collection class {}", name, e);
        } catch (InstantiationException e) {
            log.warn("Unable to create enhancers collection {}", name, e);
        } catch (IllegalAccessException e) {
            log.warn("Unable to create enhancers collection {}", name, e);
        }
        this.enhancersCollection = enhancersCollection;
        register();
    }

    private void register() {
        EnhancerRegistryPlugin registry = Bootstrap.getPlugin(EnhancerRegistryPlugin.class);
        if (registry == null) {
            log.warn("Unable to find EnhancerPluginRegistryImpl in the classpath. Will not be able to use {} enhancements", this);
            return;
        }
        registry.addEnhancerPlugin(getClass().getSimpleName().substring("EnhancerPlugin_".length()), this);
    }

    public void init(Element node, Configuration_01 conf) {
        if (enhancersCollection == null || enhancersCollection.isEmpty()) {
            log.debug("Enhancers collection is null or empty");
            return;
        }

        final EnhancementRegistry er = conf.getEnhancementRegistry();
        for (Map.Entry<String, ClassEnhancer> entry : enhancersCollection.entrySet())
            er.addEnhancer(entry.getKey(), new FilteredEnhancer(this, entry.getValue()));
    }

    public boolean accept(ClassInfo info) {
        return true;
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj.getClass() == getClass();
    }

    public void setStackTraceAtCreate(ConfigStackElement currentStack) {
        stack = currentStack;
    }

    public ConfigStackElement getStackTraceAtCreate() {
        return stack;
    }
}
