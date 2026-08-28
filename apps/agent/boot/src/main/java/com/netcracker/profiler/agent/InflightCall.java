package com.netcracker.profiler.agent;

import java.util.List;
import java.util.Map;

public interface InflightCall {
    public long time();

    public int method();

    public int duration();

    public int suspendDuration();

    public int calls();

    public int traceFileIndex();

    public int bufferOffset();

    public int recordIndex();

    public int logsGenerated();

    public int logsWritten();

    public String threadName();

    public long cpuTime();

    public long waitTime();

    public long memoryUsed();

    public Map<Integer, List<String>> params();
}
