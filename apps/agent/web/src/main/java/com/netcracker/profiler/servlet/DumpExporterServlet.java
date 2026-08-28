package com.netcracker.profiler.servlet;

import com.netcracker.profiler.io.IDumpExporter;

import java.io.*;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

@Singleton
public class DumpExporterServlet extends HttpServlet {

    private final IDumpExporter dumpExporter;

    @Inject
    public DumpExporterServlet(IDumpExporter dumpExporter) {
        this.dumpExporter = dumpExporter;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        dumpExporter.exportDump(req, resp);
    }
}
