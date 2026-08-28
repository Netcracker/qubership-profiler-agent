package com.netcracker.profiler.servlet;

import com.netcracker.profiler.io.ActivePODReport;
import com.netcracker.profiler.io.IActivePODReporter;
import com.netcracker.profiler.io.TemporalRequestParams;
import com.netcracker.profiler.io.TemporalUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

@Singleton
public class ActivePODsFetcher extends HttpServlet {
    private static final Logger logger = LoggerFactory.getLogger(ActivePODsFetcher.class);

    private final IActivePODReporter reporter;

    @Inject
    public ActivePODsFetcher(IActivePODReporter reporter) {
        this.reporter = reporter;
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        TemporalRequestParams temporal = TemporalUtils.parseTemporal(req);
        String searchConditions = req.getParameter("searchConditions");
        List<ActivePODReport> report = reporter.reportActivePODs(searchConditions, temporal);
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.writeValue(resp.getOutputStream(), report);
    }
}
