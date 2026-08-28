package com.netcracker.profiler.io;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

public class LimitedInputStream extends FilterInputStream {
    long pos;
    private final long maxSize;

    /**
     * Creates a new filtered reader.
     *
     * @param in a Reader object providing the underlying stream.
     * @param maxSize number of bytes to read
     * @throws NullPointerException if <code>in</code> is <code>null</code>
     */
    public LimitedInputStream(InputStream in, long maxSize) {
        super(in);
        this.maxSize = maxSize;
    }

    @Override
    public int read() throws IOException {
        if (pos > maxSize) return -1;
        pos++;
        return super.read();
    }

    @Override
    public int read(byte[] cbuf, int off, int len) throws IOException {
        if (pos > maxSize) return -1;
        final int read = super.read(cbuf, off, len);
        pos += read;
        return read;
    }

    @Override
    public long skip(long n) throws IOException {
        if (pos > maxSize) return -1;
        final long skip = super.skip(n);
        pos += skip;
        return skip;
    }

    public long position(){
        return pos;
    }
}
