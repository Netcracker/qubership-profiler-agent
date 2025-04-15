package org.qubership.profiler.io;

import java.io.IOException;
import java.io.OutputStream;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public interface IDumpExporter {
    void exportDump(HttpServletRequest req, HttpServletResponse resp) throws IOException;
    void exportGC(String podName, String streamName, OutputStream out, TemporalRequestParams temporal) throws IOException;
}
