package com.netcracker.profiler.sax.io;

import java.io.IOException;

public interface IDataInputStreamEx extends AutoCloseable {
    long readLong() throws IOException;

    int readVarInt() throws IOException;

    long readVarLong() throws IOException;

    int readVarIntZigZag() throws IOException;

    int position();

    int available() throws IOException;

    int read() throws IOException;

    String readString() throws IOException;

    void skipString() throws IOException;
}
