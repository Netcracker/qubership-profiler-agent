package com.netcracker.profiler.agent;

import java.lang.reflect.Constructor;
import java.util.logging.Level;

public class ThreadJMXProviderFactory {
    private static boolean DEBUG = Boolean.getBoolean(ThreadJMXProviderFactory.class.getName() + ".debug");
    private static final ESCLogger logger = ESCLogger.getLogger(ThreadJMXProviderFactory.class, (DEBUG ? Level.FINE : ESCLogger.ESC_LOG_LEVEL));
    public static final ThreadJMXProvider INSTANCE = create();

    private static ThreadJMXProvider create() {
        try {
            if (ProfilerData.THREAD_CPU || ProfilerData.THREAD_WAIT || ProfilerData.THREAD_MEMORY) {
                Class.forName("java.lang.management.ManagementFactory");
                final Class<?> tester = Class.forName("com.netcracker.profiler.agent.FindJMXImplementation");
                String[] classNames = (String[]) tester.getMethod("find").invoke(null);
                if (classNames != null) {
                    final Class<?> klass = Class.forName("com.netcracker.profiler.agent.ThreadJMX");
                    final Constructor<?> cons = klass.getConstructor(String[].class);
                    return (ThreadJMXProvider) cons.newInstance(new Object[]{classNames});
                }
            }
        } catch (Throwable e) {
            if (DEBUG)
                logger.log(Level.FINE, "", e);
        }

        try {
            return (ThreadJMXProvider) Class.forName("com.netcracker.profiler.agent.ThreadJMXEmpty").newInstance();
        } catch (Throwable e) {
            throw new IllegalStateException("Unable to instantiate empty jmx provider", e);
        }
    }
}
