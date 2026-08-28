package com.netcracker.profiler.servlet;

import com.netcracker.profiler.io.ExcelExporter;
import com.netcracker.profiler.io.TemporalRequestParams;
import com.netcracker.profiler.io.TemporalUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

@Singleton
public class ExcelExporterServlet extends HttpServlet {
    private static final Logger log = LoggerFactory.getLogger(ExcelExporterServlet.class);

    private final ExcelExporter excelExporter;

    @Inject
    public ExcelExporterServlet(ExcelExporter excelExporter) {
        this.excelExporter = excelExporter;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        TemporalRequestParams temporal = TemporalUtils.parseTemporal(req);
        String serverAddress = req.getScheme() + "://" + req.getServerName() + ":" + req.getServerPort() + req.getContextPath();
        resp.setContentType("application/x-msdownload");
        resp.setHeader("Content-disposition", "attachment; filename=calls.xlsx");
        Map<String, String[]> params = new HashMap<>(req.getParameterMap());
        splitParams(params, "nodes", "urlReplacePatterns");
        excelExporter.export(temporal, params, resp.getOutputStream(), serverAddress);
    }

    private void splitParams(Map<String, String[]> params, String ... keys) {
        for(String key : keys) {
            String[] vals = params.get(key);
            if(vals == null || vals.length < 1) {
                continue;
            }
            String[] splitted = vals[0].trim().split(" ");
            if(splitted.length == 1 && splitted[0].isEmpty()) {
                splitted = new String[]{};
            }
            params.put(key, splitted);
        }
    }
}
