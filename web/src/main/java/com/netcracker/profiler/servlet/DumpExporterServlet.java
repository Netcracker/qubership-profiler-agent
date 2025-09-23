package com.netcracker.profiler.servlet;

import com.netcracker.profiler.io.IDumpExporter;

import java.io.*;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class DumpExporterServlet extends HttpServlet {


    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        IDumpExporter exporter = SpringBootInitializer.dumpExporter();

        exporter.exportDump(req, resp);
    }


}
