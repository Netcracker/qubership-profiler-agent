package com.netcracker.profiler.agent;

import java.io.IOException;

public interface DumperCollectorClient extends AutoCloseable{
    DumperRemoteControlledStream createRollingChunk(final String streamName, int requestedRollingSequenceId, boolean resetRequired) throws IOException;
    void write(byte[] bytes, int offset, int length, String streamName) throws IOException;
    void flush() throws IOException;
    void close() throws IOException;
    boolean isOnline();
    String getPodName();
    void requestAckFlush(boolean doFlush) throws IOException;
    boolean validateWriteDataAcks(boolean sync) throws IOException;
}
