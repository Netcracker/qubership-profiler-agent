package org.qubership.profiler.io;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.OutputStream;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@Component
@Profile("filestorage")
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
