package com.netcracker.profiler.io;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

public class InputStreamNoCloseAdapter extends FilterInputStream {
    /**
     * Creates a <code>FilterInputStream</code>
     * by assigning the  argument <code>in</code>
     * to the field <code>this.in</code> so as
     * to remember it for later use.
     *
     * @param in the underlying input stream, or <code>null</code> if
     *           this instance is to be created without an underlying stream.
     */
    public InputStreamNoCloseAdapter(InputStream in) {
        super(in);
    }

    @Override
    public void close() throws IOException {
        // Do not close underlying input stream
    }
}
