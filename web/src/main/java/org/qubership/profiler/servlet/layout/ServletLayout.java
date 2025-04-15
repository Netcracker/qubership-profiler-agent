package org.qubership.profiler.servlet.layout;

import org.qubership.profiler.output.layout.Layout;

import java.io.IOException;
import java.io.OutputStream;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;

public class ServletLayout extends Layout {
    protected HttpServletResponse resp;
    protected ServletOutputStream out;

    private final String encoding;
    private final String contentType;
    private boolean contentTypeSet;

    public ServletLayout(HttpServletResponse resp) {
        this(resp, "UTF-8", "text/html");
    }

    public ServletLayout(HttpServletResponse resp, String encoding, String contentType) {
        this.resp = resp;
        this.encoding = encoding;
        this.contentType = contentType;
    }

    public String getEncoding() {
        return encoding;
    }

    @Override
    public OutputStream getOutputStream() throws IOException {
        if (!contentTypeSet) {
            contentTypeSet = true;
            resp.setContentType(contentType + (encoding != null ? "; charset=" + encoding : ""));
        }
        out = resp.getOutputStream();
        return out;
    }

    @Override
    public void putNextEntry(String id, String name, String type) {

    }

    @Override
    public void close() throws IOException {
        if (out == null)
            return;

        out.close();
        out = null;
    }
}
