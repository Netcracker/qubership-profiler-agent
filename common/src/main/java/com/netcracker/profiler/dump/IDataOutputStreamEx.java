package com.netcracker.profiler.dump;

import java.io.Closeable;
import java.io.DataOutput;
import java.io.Flushable;
import java.io.IOException;

public interface IDataOutputStreamEx extends Closeable, Flushable, DataOutput {
    int getPrevStringOffset();

    int write(String s) throws IOException;

    int writeVarInt(int i) throws IOException;

    int writeVarInt(long j) throws IOException;

    int writeVarIntZigZag(int src) throws IOException;

    int writeVarIntZigZag(long src) throws IOException;

    int size();
}
