package com.netcracker.profiler.guice;

import com.netcracker.profiler.audit.UsernameFilter;
import com.netcracker.profiler.filter.*;
import com.netcracker.profiler.security.DummyAuthenticationFilter;
import com.netcracker.profiler.security.DummySecurityFilter;
import com.netcracker.profiler.security.DummySecurityService;
import com.netcracker.profiler.security.csrf.CSRFGuardFilter;
import com.netcracker.profiler.security.csrf.CsrfTokenServlet;
import com.netcracker.profiler.servlet.*;

import com.google.inject.Key;
import com.google.inject.servlet.ServletModule;

import java.io.File;

/**
 * Guice module for web layer configuration.
 * Configures servlets, filters, and web-specific dependencies.
 * @see <a href="https://github.com/google/guice/wiki/ServletModule">ServletModule</a>
 */
public class WebModule extends ServletModule {
    private final File serverDumpLocation;

    public WebModule(File serverDumpLocation) {
        this.serverDumpLocation = serverDumpLocation;
    }

    @Override
    protected void configureServlets() {
        install(new ParsersModule(serverDumpLocation));

        bind(Key.get(String.class, IsReadFromDump.class)).toInstance("true");

        // Security service - now managed by Guice
        bind(DummySecurityService.class);

        // Bind servlets with constructor injection
        // Servlets are bound to their URL patterns
        filter("/*").through(Slf4jMDCFilter.class);
        filter("/*").through(DummyAuthenticationFilter.class);
        filter("/*").through(DummySecurityFilter.class);
        filter("/*").through(UsernameFilter.class);
        filter("/*").through(CachingFilter.class);
        filter("/*").through(AddDefaultHeadersFilter.class);
        filter("/").through(AddContentTypeForHtmlFilesFilter.class);
        filter("*.html").through(AddContentTypeForHtmlFilesFilter.class);
        filter("*.html").through(GzipResponseFilter.class);
        filter("/tree").through(GzipResponseFilter.class);
        filter("*.js").through(GzipResponseFilter.class);
        filter("*.css").through(GzipResponseFilter.class);
        filter("/js/calls.js").through(ProfilerTimeoutFilter.class);
        filter("/tree/*").through(ProfilerTimeoutFilter.class);
        filter("/js/tree.js").through(ProfilerTimeoutFilter.class);
        filter("/*").through(CSRFGuardFilter.class);
        filter("/exportExcel").through(ProfilerTimeoutFilter.class);

        serve("/config/*").with(Config.class);
        serve("/js/calls.js").with(CallFetcher.class);
        serve("/get_clob/*").with(RawData.class);
        serve("/tree/*").with(TreeFetcher.class);
        serve("/time").with(Time.class);
        serve("/js/tree.js").with(TreeFetcher.class);
        serve("/metrics/*").with(Metrics.class);
        serve("/threaddump/*").with(ThreadDump.class);
        serve("/fetchActivePods").with(ActivePODsFetcher.class);
        serve("/exportDump").with(DumpExporterServlet.class);
        serve("/downloadStream").with(StreamsDownloaderServlet.class);
        serve("/exportExcel").with(ExcelExporterServlet.class);
        serve("/api/csrf-token").with(CsrfTokenServlet.class);
    }
}
