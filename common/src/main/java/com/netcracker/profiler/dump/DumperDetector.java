package com.netcracker.profiler.dump;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * This one should not depend on dumper in any way but it should detect whether profiler agent is present in the system
 * because bootstrap and dumper interfaces may be absent in the classpath
 */
public class DumperDetector {
    private static final Logger logger = LoggerFactory.getLogger(DumperDetector.class);
    //if any of the required classes are not loaded, dumper is guaranteed to be absent
    private static boolean dumperAbsent;

    public static boolean dumperActive() {
        if(dumperAbsent) {
            return false;
        }
        try {
            Class bootstrap = Class.forName("com.netcracker.profiler.agent.Bootstrap");
            Class dumperClass = Class.forName("com.netcracker.profiler.agent.DumperPlugin");
            Method getPluginOrNull = bootstrap.getDeclaredMethod("getPlugin", Class.class);
            Object dumperPlugin10 = getPluginOrNull.invoke(null, dumperClass);
            if(dumperPlugin10 == null) {
                logger.debug("Bootstrap is present in the classpath, but dumper is not");
                return false;
            }
            Method isInitializedMethod = dumperPlugin10.getClass().getDeclaredMethod("isInitialized");
            boolean result = (Boolean)isInitializedMethod.invoke(dumperPlugin10);
            logger.debug("Dumper is present in the system. Active: {}", result);
            return result;
        }catch (NoClassDefFoundError | ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            logger.debug("Dumper is either too old or absent", e);
            dumperAbsent = true;
        }
        return false;
    }
}
