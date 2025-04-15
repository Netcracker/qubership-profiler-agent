package org.qubership.profiler.servlet.layout;

import org.qubership.profiler.output.layout.Layout;

import java.io.IOException;
import java.io.OutputStream;

import javax.servlet.http.HttpServletResponse;

public class SimpleLayout extends Layout {
    private final HttpServletResponse resp;

    public SimpleLayout(HttpServletResponse resp) {
        this.resp = resp;
    }

    @Override
    public OutputStream getOutputStream() throws IOException {
        return resp.getOutputStream();
    }

    @Override
    public void putNextEntry(String id, String name, String type) throws IOException {
        System.out.println("name = " + name + ", " + type);
    }
}
