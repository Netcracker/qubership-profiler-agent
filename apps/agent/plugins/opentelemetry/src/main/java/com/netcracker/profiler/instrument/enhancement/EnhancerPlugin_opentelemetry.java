package com.netcracker.profiler.instrument.enhancement;

public class EnhancerPlugin_opentelemetry extends EnhancerPlugin {
    /**
     * Leaves the SDK that the OpenTelemetry Java agent carries to the {@code opentelemetry_javaagent}
     * plugin.
     *
     * <p>The agent keeps the SDK classes under their own names and compiles them against a
     * relocated API, so the hooks here, which are compiled against the API as published, would
     * fail to link on every span.</p>
     */
    @Override
    public boolean accept(ClassInfo info) {
        if (!info.getClassName().startsWith("io/opentelemetry/sdk/")) {
            return true;
        }
        String premainClass = info.getJarAttribute("Premain-Class");
        return premainClass == null || !premainClass.startsWith("io.opentelemetry.javaagent.");
    }
}
