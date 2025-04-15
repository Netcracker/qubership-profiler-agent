package org.qubership.profiler.servlet;

import org.qubership.profiler.io.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class ExcelExporterServlet extends HttpServlet {
    private static final Logger log = LoggerFactory.getLogger(ExcelExporterServlet.class);

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        TemporalRequestParams temporal = TemporalUtils.parseTemporal(req);
        String serverAddress = req.getScheme() + "://" + req.getServerName() + ":" + req.getServerPort() + req.getContextPath();
        resp.setContentType("application/x-msdownload");
        resp.setHeader("Content-disposition", "attachment; filename=calls.xlsx");
        Map<String, String[]> params = new HashMap<>(req.getParameterMap());
        splitParams(params, "nodes", "urlReplacePatterns");
        SpringBootInitializer.excelExporter().export(temporal, params, resp.getOutputStream(), serverAddress);
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
