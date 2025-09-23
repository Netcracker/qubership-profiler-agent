package com.netcracker.profiler.output.layout;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class ZipLayout extends LayoutDelegate {
    private ZipOutputStream zip;

    public ZipLayout(Layout parent) {
        super(parent);
    }

    @Override
    public OutputStream getOutputStream() throws IOException {
        if (zip != null)
            return zip;
        zip = new ZipOutputStream(super.getOutputStream());
        return zip;
    }

    @Override
    public void putNextEntry(String id, String name, String type) throws IOException {
        getOutputStream();
        zip.putNextEntry(new ZipEntry(name));
    }

    @Override
    public void close() throws IOException {
        getOutputStream();
        zip.close();
    }
}
