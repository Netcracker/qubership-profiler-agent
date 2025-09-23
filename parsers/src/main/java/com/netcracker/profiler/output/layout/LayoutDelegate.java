package com.netcracker.profiler.output.layout;

import java.io.IOException;
import java.io.OutputStream;

public class LayoutDelegate extends Layout {
    protected Layout parent;

    public LayoutDelegate(Layout parent) {
        this.parent = parent;
    }

    @Override
    public OutputStream getOutputStream() throws IOException {
        return parent.getOutputStream();
    }

    @Override
    public void putNextEntry(String id, String name, String type) throws IOException {
        parent.putNextEntry(id, name, type);
    }

    @Override
    public void close() throws IOException {
        parent.close();
    }
}
