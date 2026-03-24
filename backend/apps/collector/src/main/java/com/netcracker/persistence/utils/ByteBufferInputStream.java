package com.netcracker.persistence.utils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class ByteBufferInputStream extends InputStream {
    private final ByteBuffer buffer;

    public ByteBufferInputStream(ByteBuffer buf) {
        this.buffer = buf;
    }

    public int available() {
        return buffer.remaining();
    }

    public int read() throws IOException {
        return buffer.hasRemaining() ? buffer.get() & 255 : -1;
    }

    public int read(byte[] bytes, int off, int len) throws IOException {
        if (!buffer.hasRemaining()) {
            return -1;
        } else {
            len = Math.min(len, buffer.remaining());
            buffer.get(bytes, off, len);
            return len;
        }
    }
}
