package com.netcracker.profiler.servlet;

import com.netcracker.profiler.output.layout.FileAppender;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import jakarta.servlet.ServletContext;

/**
 * Appends the required files retrieved from {@link jakarta.servlet.ServletContext}.
 */
public class ServletResourceAppender implements FileAppender {
    private final ServletContext context;

    public ServletResourceAppender(ServletContext context) {
        this.context = context;
    }

    public void append(String fileName, OutputStream out) throws IOException {
        final InputStream is = context.getResourceAsStream(fileName);
        if (is == null) return;
        int read;
        byte[] buf = new byte[4096];
        try {
            while ((read = is.read(buf)) > 0)
                out.write(buf, 0, read);
        } finally {
            is.close();
        }
    }
}
