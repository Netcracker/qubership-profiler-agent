package com.netcracker.profiler.servlet;

import com.netcracker.profiler.io.IDumpExporter;
import com.netcracker.profiler.io.TemporalRequestParams;
import com.netcracker.profiler.io.TemporalUtils;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class StreamsDownloaderServlet extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        TemporalRequestParams temporal = TemporalUtils.parseTemporal(req);
        IDumpExporter exporter = SpringBootInitializer.dumpExporter();
        String podName = req.getParameter("podName");
        String streamName = req.getParameter("streamName");

        resp.setContentType("application/x-msdownload");
        resp.setHeader("Content-disposition", "attachment; filename=gc.zip");
        exporter.exportGC(podName, streamName, resp.getOutputStream(), temporal);
    }


}
