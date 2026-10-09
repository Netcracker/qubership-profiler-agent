package com.netcracker.profiler.instrument.enhancement;

public class EnhancerPlugin_opentelemetry_javaagent extends EnhancerPlugin {
    /**
     * Accepts only the SDK that the OpenTelemetry Java agent carries. The SDK an application brings
     * itself has the same class names and belongs to the {@code opentelemetry} plugin.
     */
    @Override
    public boolean accept(ClassInfo info) {
        String premainClass = info.getJarAttribute("Premain-Class");
        return premainClass != null && premainClass.startsWith("io.opentelemetry.javaagent.");
    }
}
