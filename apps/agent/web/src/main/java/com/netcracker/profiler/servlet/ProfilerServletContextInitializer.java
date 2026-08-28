package com.netcracker.profiler.servlet;

import com.netcracker.profiler.dump.DumpRootResolver;
import com.netcracker.profiler.guice.WebModule;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.servlet.GuiceServletContextListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import jakarta.servlet.ServletContextEvent;

/**
 * Guice-based initializer for the Profiler web application.
 * Extends GuiceServletContextListener to enable Guice servlet integration.
 */
public class ProfilerServletContextInitializer extends GuiceServletContextListener {
    public static final String DUMP_ROOT_PROPERTY = "com.netcracker.profiler.DUMP_ROOT_LOCATION";
    public static final String IS_READ_FROM_DUMP = "com.netcracker.profiler.IS_READ_FROM_DUMP";

    private static final Logger log = LoggerFactory.getLogger(ProfilerServletContextInitializer.class);

    @Override
    protected Injector getInjector() {
        log.info("Initializing Profiler Guice config");
        try {
            File serverDumpLocation = new File(DumpRootResolver.dumpRoot);
            return Guice.createInjector(new WebModule(serverDumpLocation));
        } catch (Throwable e) {
            log.error("Failed to initialize Profiler", e);
            throw new RuntimeException("Profiler initialization failed", e);
        }
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        super.contextDestroyed(sce);
        log.info("Profiler context destroyed");
    }
}
