package com.netcracker.profiler.io;

import java.io.IOException;
import java.io.OutputStream;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public interface IDumpExporter {
    void exportDump(HttpServletRequest req, HttpServletResponse resp) throws IOException;
    void exportGC(String podName, String streamName, OutputStream out, TemporalRequestParams temporal) throws IOException;
}
