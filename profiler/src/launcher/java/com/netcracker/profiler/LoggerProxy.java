package com.netcracker.profiler;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;

public class LoggerProxy {
    public static final String LOGGER_FACTORY_CLASS = "org.slf4j.LoggerFactory";
    public static final String LOGGER_FACTORY_CLASS_SHADED = "com.netcracker.profiler.shaded.org.slf4j.LoggerFactory";

    private Object logger;

    public LoggerProxy(Class clazz, ClassLoader classLoader) throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        classLoader.loadClass("com.netcracker.profiler.servlet.LogbackInitializer").getDeclaredMethod("initLogback").invoke(null);
        Class loggerClazz;
        try {
            loggerClazz = classLoader.loadClass(LOGGER_FACTORY_CLASS);
        } catch (ClassNotFoundException e) {
            try {
                loggerClazz = classLoader.loadClass(LOGGER_FACTORY_CLASS_SHADED);
            }catch (ClassNotFoundException e2) {
                e2.printStackTrace();
                return;
            }
        }
        logger = loggerClazz.getDeclaredMethod("getLogger", Class.class).invoke(null, clazz);

    }

    private void invoke(String methodName, Class[] argsClasses, Object ... args) {
        if(logger == null) {
            System.out.println(methodName + ": " + Arrays.asList(args));
        }
        try{
            logger.getClass().getDeclaredMethod(methodName, argsClasses).invoke(logger, args);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void debug(String msg) {
        invoke("debug", new Class[]{String.class}, msg);
    }

    public void debug(String msg, Object ... args) {
        invoke("debug", new Class[]{String.class, Object[].class}, msg, args);
    }

    public void debug(String msg, Throwable t) {
        invoke("debug", new Class[]{String.class, Throwable.class}, msg, t);
    }

    public void info(String msg) {
        invoke("info", new Class[]{String.class}, msg);
    }

    public void info(String msg, Object ... args) {
        invoke("info", new Class[]{String.class, Object[].class}, msg, args);
    }

    public void info(String msg, Throwable t) {
        invoke("info", new Class[]{String.class, Throwable.class}, msg, t);
    }

    public void warn(String msg) {
        invoke("warn", new Class[]{String.class}, msg);
    }

    public void warn(String msg, Object ... args) {
        invoke("warn", new Class[]{String.class, Object[].class}, msg, args);
    }

    public void warn(String msg, Throwable t) {
        invoke("warn", new Class[]{String.class, Throwable.class}, msg, t);
    }

    public void error(String msg) {
        invoke("error", new Class[]{String.class}, msg);
    }

    public void error(String msg, Object ... args) {
        invoke("error", new Class[]{String.class, Object[].class}, msg, args);
    }

    public void error(String msg, Throwable t) {
        invoke("error", new Class[]{String.class, Throwable.class}, msg, t);
    }

}
