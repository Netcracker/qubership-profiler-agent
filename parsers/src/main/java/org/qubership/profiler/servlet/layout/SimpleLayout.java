package org.qubership.profiler.servlet.layout;

import org.qubership.profiler.output.layout.Layout;

import java.io.IOException;
import java.io.OutputStream;

import javax.servlet.http.HttpServletResponse;

public class SimpleLayout extends Layout {
    private final OutputStream out;

    public SimpleLayout(OutputStream out) {
        this.out = out;
    }

    @Override
    public OutputStream getOutputStream() throws IOException {
        return out;
    }

    @Override
    public void putNextEntry(String id, String name, String type) throws IOException {
        System.out.println("name = " + name + ", " + type);
    }

    @Override
    public void close() throws IOException {
        out.close();
    }
}
