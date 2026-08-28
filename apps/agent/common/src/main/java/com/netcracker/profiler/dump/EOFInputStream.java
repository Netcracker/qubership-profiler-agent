package com.netcracker.profiler.dump;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

public class EOFInputStream extends InputStream {

    @Override
    public int read() throws IOException {
        throw new EOFException();
    }

}
