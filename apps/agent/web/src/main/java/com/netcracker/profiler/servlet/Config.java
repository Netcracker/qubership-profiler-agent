package com.netcracker.profiler.servlet;

import java.io.IOException;

import jakarta.inject.Singleton;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

@Singleton
public class Config extends HttpServlet {
    public final ConfigImpl impl = new ConfigImpl();

    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        final String pathInfo = request.getPathInfo();
        if ("/reload".equals(pathInfo))
            impl.processConfigurationReload(request, response);
        if ("/dumper/stop".equals(pathInfo))
            impl.stopDumper(request, response, null);
        if ("/dumper/start".equals(pathInfo))
            impl.startDumper(request, response, null);
        if ("/dumper/rescan".equals(pathInfo))
            impl.rescanDumpFiles(request, response, null);
    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        final String pathInfo = request.getPathInfo();
        if ("/reload_status".equals(pathInfo))
            impl.showReloadStatus(request, response, null);
        if ("/dumper/status".equals(pathInfo))
            impl.showDumperStatus(request, response, null);
    }
}
