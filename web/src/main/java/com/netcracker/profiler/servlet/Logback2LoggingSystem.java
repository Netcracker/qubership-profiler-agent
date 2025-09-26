package com.netcracker.profiler.servlet;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.turbo.TurboFilter;
import ch.qos.logback.core.spi.FilterReply;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.springframework.boot.logging.LogFile;
import org.springframework.boot.logging.LogLevel;
import org.springframework.boot.logging.LoggerConfiguration;
import org.springframework.boot.logging.LoggingInitializationContext;
import org.springframework.boot.logging.LoggingSystem;
import org.springframework.boot.logging.LoggingSystemFactory;
import org.springframework.boot.logging.LoggingSystemProperties;
import org.springframework.boot.logging.Slf4JLoggingSystem;
import org.springframework.boot.logging.logback.LogbackLoggingSystemProperties;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

import java.security.CodeSource;
import java.security.ProtectionDomain;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class Logback2LoggingSystem extends Slf4JLoggingSystem {
    private static final String CONFIGURATION_FILE_PROPERTY = "logback.configurationFile";

    private static final LogLevels<Level> LEVELS = new LogLevels<>();

    static {
        LEVELS.map(LogLevel.TRACE, Level.TRACE);
        @SuppressWarnings("deprecation")
        Level all = Level.ALL;
        LEVELS.map(LogLevel.TRACE, all);
        LEVELS.map(LogLevel.DEBUG, Level.DEBUG);
        LEVELS.map(LogLevel.INFO, Level.INFO);
        LEVELS.map(LogLevel.WARN, Level.WARN);
        LEVELS.map(LogLevel.ERROR, Level.ERROR);
        LEVELS.map(LogLevel.FATAL, Level.ERROR);
        LEVELS.map(LogLevel.OFF, Level.OFF);
    }

    private static final TurboFilter FILTER = new TurboFilter() {

        @Override
        public FilterReply decide(Marker marker, ch.qos.logback.classic.Logger logger, Level level, String format,
                                  Object[] params, Throwable t) {
            return FilterReply.DENY;
        }

    };

    public Logback2LoggingSystem(ClassLoader classLoader) {
        super(classLoader);
    }

    @Override
    public LoggingSystemProperties getSystemProperties(ConfigurableEnvironment environment) {
        return new LogbackLoggingSystemProperties(environment);
    }

    @Override
    protected String[] getStandardConfigLocations() {
        return new String[] { "logback-test.groovy", "logback-test.xml", "logback.groovy", "logback.xml" };
    }

    @Override
    public void beforeInitialize() {
        LoggerContext loggerContext = getLoggerContext();
        if (isAlreadyInitialized(loggerContext)) {
            return;
        }
        super.beforeInitialize();
        loggerContext.getTurboFilterList().add(FILTER);
    }

    @Override
    public void initialize(LoggingInitializationContext initializationContext, String configLocation, LogFile logFile) {
        LoggerContext loggerContext = getLoggerContext();
        if (isAlreadyInitialized(loggerContext)) {
            return;
        }
        super.initialize(initializationContext, configLocation, logFile);
        loggerContext.getTurboFilterList().remove(FILTER);
        markAsInitialized(loggerContext);
        if (StringUtils.hasText(System.getProperty(CONFIGURATION_FILE_PROPERTY))) {
            getLogger(Logback2LoggingSystem.class.getName()).warn("Ignoring '" + CONFIGURATION_FILE_PROPERTY
                    + "' system property. Please use 'logging.config' instead.");
        }
    }

    @Override
    protected void loadDefaults(LoggingInitializationContext initializationContext, LogFile logFile) {
        LogbackInitializer.initLogback();
    }

    @Override
    protected void loadConfiguration(LoggingInitializationContext initializationContext, String location,
                                     LogFile logFile) {
        LogbackInitializer.initLogback();
    }

    @Override
    public void cleanUp() {
        LoggerContext context = getLoggerContext();
        markAsUninitialized(context);
        super.cleanUp();
        context.getStatusManager().clear();
        context.getTurboFilterList().remove(FILTER);
    }

    @Override
    protected void reinitialize(LoggingInitializationContext initializationContext) {
        getLoggerContext().reset();
        getLoggerContext().getStatusManager().clear();
        loadConfiguration(initializationContext, getSelfInitializationConfig(), null);
    }

    @Override
    public List<LoggerConfiguration> getLoggerConfigurations() {
        List<LoggerConfiguration> result = new ArrayList<>();
        for (ch.qos.logback.classic.Logger logger : getLoggerContext().getLoggerList()) {
            result.add(getLoggerConfiguration(logger));
        }
        result.sort(CONFIGURATION_COMPARATOR);
        return result;
    }

    @Override
    public LoggerConfiguration getLoggerConfiguration(String loggerName) {
        String name = getLoggerName(loggerName);
        LoggerContext loggerContext = getLoggerContext();
        return getLoggerConfiguration(loggerContext.exists(name));
    }

    private String getLoggerName(String name) {
        if (!StringUtils.hasLength(name) || Logger.ROOT_LOGGER_NAME.equals(name)) {
            return ROOT_LOGGER_NAME;
        }
        return name;
    }

    private LoggerConfiguration getLoggerConfiguration(ch.qos.logback.classic.Logger logger) {
        if (logger == null) {
            return null;
        }
        LogLevel level = LEVELS.convertNativeToSystem(logger.getLevel());
        LogLevel effectiveLevel = LEVELS.convertNativeToSystem(logger.getEffectiveLevel());
        String name = getLoggerName(logger.getName());
        return new LoggerConfiguration(name, level, effectiveLevel);
    }

    @Override
    public Set<LogLevel> getSupportedLogLevels() {
        return LEVELS.getSupported();
    }

    @Override
    public void setLogLevel(String loggerName, LogLevel level) {
        ch.qos.logback.classic.Logger logger = getLogger(loggerName);
        if (logger != null) {
            logger.setLevel(LEVELS.convertSystemToNative(level));
        }
    }

    @Override
    public Runnable getShutdownHandler() {
        return () -> getLoggerContext().stop();
    }

    private ch.qos.logback.classic.Logger getLogger(String name) {
        LoggerContext factory = getLoggerContext();
        return factory.getLogger(getLoggerName(name));
    }

    private LoggerContext getLoggerContext() {
        ILoggerFactory factory = LoggerFactory.getILoggerFactory();
        Assert.isInstanceOf(LoggerContext.class, factory,
                () -> String.format(
                        "LoggerFactory is not a Logback LoggerContext but Logback is on "
                                + "the classpath. Either remove Logback or the competing "
                                + "implementation (%s loaded from %s). If you are using "
                                + "WebLogic you will need to add 'org.slf4j' to "
                                + "prefer-application-packages in WEB-INF/weblogic.xml",
                        factory.getClass(), getLocation(factory)));
        return (LoggerContext) factory;
    }

    private Object getLocation(ILoggerFactory factory) {
        try {
            ProtectionDomain protectionDomain = factory.getClass().getProtectionDomain();
            CodeSource codeSource = protectionDomain.getCodeSource();
            if (codeSource != null) {
                return codeSource.getLocation();
            }
        }
        catch (SecurityException ex) {
            // Unable to determine location
        }
        return "unknown location";
    }

    private boolean isAlreadyInitialized(LoggerContext loggerContext) {
        return loggerContext.getObject(LoggingSystem.class.getName()) != null;
    }

    private void markAsInitialized(LoggerContext loggerContext) {
        loggerContext.putObject(LoggingSystem.class.getName(), new Object());
    }

    private void markAsUninitialized(LoggerContext loggerContext) {
        loggerContext.removeObject(LoggingSystem.class.getName());
    }

    /**
     * {@link LoggingSystemFactory} that returns {@link Logback2LoggingSystem} if possible.
     */
    @Order(Ordered.HIGHEST_PRECEDENCE)
    public static class Factory implements LoggingSystemFactory {

        private static final boolean PRESENT = ClassUtils.isPresent("ch.qos.logback.classic.LoggerContext",
                Factory.class.getClassLoader());

        @Override
        public LoggingSystem getLoggingSystem(ClassLoader classLoader) {
            if (PRESENT) {
                return new Logback2LoggingSystem(classLoader);
            }
            return null;
        }

    }

}
