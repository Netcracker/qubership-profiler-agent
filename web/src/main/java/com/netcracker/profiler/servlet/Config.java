package com.netcracker.profiler.servlet;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

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
