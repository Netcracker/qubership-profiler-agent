package com.netcracker.profiler.servlet;

import com.netcracker.profiler.audit.SessionAuditListener;
import com.netcracker.profiler.configuration.PropertyFacade;
import com.netcracker.profiler.dump.DumpRootResolver;

import ch.qos.logback.classic.BasicConfigurator;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import org.slf4j.LoggerFactory;

import java.io.File;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

public class LogbackInitializer implements ServletContextListener {
    private static final java.util.logging.Logger logger = java.util.logging.Logger.getLogger(LogbackInitializer.class.getName());

    private static boolean logbackInitialized = false;

    public void contextInitialized(ServletContextEvent servletContextEvent) {
        initLogback();
    }

    public void contextDestroyed(ServletContextEvent servletContextEvent) {
        LoggerFactory.getLogger(SessionAuditListener.class).info("Audit log stopped");
    }

    public static void initLogback() {
        if(logbackInitialized) return;

        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        String logbackXmlPath = PropertyFacade.getProperty("profiler.web.log.config", null);

        File logFile = new File(new File(DumpRootResolver.PROFILER_HOME), "config/logback-web.xml");
        if (logbackXmlPath == null) {
            if (logFile.exists()) {
                logbackXmlPath = logFile.getAbsolutePath();
            }
        }

        if (logbackXmlPath == null)
            logger.warning("Profiler: unable to find " + logFile.getAbsolutePath() + " configuration file, you might specify its location via execution-statistics-collector.web.log.config property");

        try {
            JoranConfigurator configurator = new JoranConfigurator();
            configurator.setContext(lc);
            // the context was probably already configured by default configuration rules
            lc.reset();
            if (logbackXmlPath != null) {
                logger.fine("Profiler: reading logback configuration from " + logbackXmlPath);

                configurator.doConfigure(logbackXmlPath);
            } else {
                logger.fine("Profiler: trying to find /WEB-INF/logback-web.xml in classpath");

                configurator.doConfigure(LogbackInitializer.class.getResourceAsStream("/WEB-INF/logback-web.xml"));
            }
        } catch (JoranException je) {
            logger.log(java.util.logging.Level.WARNING, "Unable to load logging configuration from " + logbackXmlPath, je);

            new BasicConfigurator().configure(lc);
            Logger root = lc.getLogger("ROOT");
            String logLevel = System.getProperty("esc.web.log.level");
            logger.warning("logLevel 2 = " + logLevel);
            if (logLevel != null){
                Level newLevel = Level.toLevel(logLevel);
                logger.fine("Profiler: activating " + newLevel + " log level for the web application");
                root.setLevel(newLevel);
            }
        }
        LoggerFactory.getLogger(SessionAuditListener.class).info("Audit log started");
        logbackInitialized = true;
    }

}
