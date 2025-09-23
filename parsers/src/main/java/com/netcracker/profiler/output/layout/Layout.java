package com.netcracker.profiler.output.layout;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

public abstract class Layout implements Closeable {
    public static final String CLOB = "clob";

    public abstract OutputStream getOutputStream() throws IOException;

    public abstract void putNextEntry(String id, String name, String type) throws IOException;

    public void close() throws IOException {
    }
}
