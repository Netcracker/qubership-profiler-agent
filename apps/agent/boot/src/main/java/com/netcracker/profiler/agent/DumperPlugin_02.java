package com.netcracker.profiler.agent;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;

public interface DumperPlugin_02 extends DumperPlugin_01 {
    public void newDumper(BlockingQueue<LocalBuffer> dirtyBuffers, BlockingQueue<LocalBuffer> emptyBuffers, ConcurrentMap<Thread, LocalState> activeThreads);

    public boolean start();

    public boolean stop(boolean force);

    public boolean isStarted();

    public int getNumberOfRestarts();

    public long getWrittenRecords();

    public long getWrittenBytes();

    public long getWriteTime();

    public long getDumperStartTime();
}
