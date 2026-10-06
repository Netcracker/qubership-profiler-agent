package com.netcracker.profiler.dump;

import java.io.IOException;

public interface IDataInputStreamEx {
    long readLong() throws IOException;

    int readVarInt() throws IOException;

    int position();

    int available() throws IOException;

    void reset() throws IOException;

    int read() throws IOException;

    String readString() throws IOException;

    void skipString() throws IOException;
}
