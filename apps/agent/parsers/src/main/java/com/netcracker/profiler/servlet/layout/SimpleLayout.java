package com.netcracker.profiler.servlet.layout;

import com.netcracker.profiler.output.layout.Layout;

import java.io.IOException;
import java.io.OutputStream;

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
    }

    @Override
    public void close() throws IOException {
        out.close();
    }
}
