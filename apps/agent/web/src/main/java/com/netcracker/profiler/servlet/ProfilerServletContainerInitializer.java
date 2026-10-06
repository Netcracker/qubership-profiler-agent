package com.netcracker.profiler.servlet;

import com.netcracker.profiler.audit.SessionAuditListener;

import com.google.inject.servlet.GuiceFilter;

import java.util.EnumSet;

import jakarta.servlet.FilterRegistration;
import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletException;

/**
 * Programmatic web application initializer that configures Guice for servlet support.
 * This class registers the GuiceFilter to intercept all requests.
 */
public class ProfilerServletContainerInitializer implements jakarta.servlet.ServletContainerInitializer {

    @Override
    public void onStartup(java.util.Set<Class<?>> c, ServletContext ctx) throws ServletException {
        ctx.addListener(LogbackInitializer.class);
        ctx.addListener(SessionAuditListener.class);
        ctx.addListener(ProfilerServletContextInitializer.class);

        // Register GuiceFilter to intercept all requests for dependency injection
        FilterRegistration.Dynamic guiceFilter = ctx.addFilter("guiceFilter", GuiceFilter.class);
        guiceFilter.addMappingForUrlPatterns(
                EnumSet.allOf(jakarta.servlet.DispatcherType.class),
                true,
                "/*"
        );
    }
}
