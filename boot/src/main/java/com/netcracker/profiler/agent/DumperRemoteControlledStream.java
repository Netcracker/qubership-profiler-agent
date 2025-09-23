package com.netcracker.profiler.agent;

import java.io.OutputStream;

public interface DumperRemoteControlledStream extends AutoCloseable{
    String getStreamName();

    long getRotationPeriod();

    long getRequiredRotationSize();
    int getRollingSequenceId();

    OutputStream getOutputStream();
    DumperCollectorClient getClient();
}
