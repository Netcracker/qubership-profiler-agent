package com.netcracker.profiler.servlet;

import com.netcracker.profiler.io.IDumpExporter;
import com.netcracker.profiler.io.TemporalRequestParams;
import com.netcracker.profiler.io.TemporalUtils;

import java.io.IOException;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

@Singleton
public class StreamsDownloaderServlet extends HttpServlet {

    private final IDumpExporter dumpExporter;

    @Inject
    public StreamsDownloaderServlet(IDumpExporter dumpExporter) {
        this.dumpExporter = dumpExporter;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        TemporalRequestParams temporal = TemporalUtils.parseTemporal(req);
        String podName = req.getParameter("podName");
        String streamName = req.getParameter("streamName");

        resp.setContentType("application/x-msdownload");
        resp.setHeader("Content-disposition", "attachment; filename=gc.zip");
        dumpExporter.exportGC(podName, streamName, resp.getOutputStream(), temporal);
    }
}
