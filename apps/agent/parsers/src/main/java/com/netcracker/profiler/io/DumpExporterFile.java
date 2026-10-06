package com.netcracker.profiler.io;

import java.io.IOException;
import java.io.OutputStream;

import jakarta.inject.Singleton;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

@Singleton
public class DumpExporterFile implements IDumpExporter {
    @Override
    public void exportDump(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        throw new RuntimeException("Not supported for file storage");
    }

    @Override
    public void exportGC(String podName, String streamName, OutputStream out, TemporalRequestParams temporal) throws IOException {
        throw new RuntimeException("Not supported for file storage");
    }
}
