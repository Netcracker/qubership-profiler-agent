package com.netcracker.profiler.io;

import com.netcracker.profiler.agent.InflightCall_02;

import java.util.List;
import java.util.Map;

public class InflightCallImpl implements InflightCall_02 {
    public long time;
    public int method;
    public int duration;
    public int queueWaitDuration; // the duration when work request waited in the queue
    public int suspendDuration;
    public int calls;
    public int traceFileIndex;
    public int bufferOffset;
    public int recordIndex;
    public int logsGenerated, logsWritten;
    public String threadName;
    public long cpuTime;
    public long waitTime;
    public long memoryUsed;
    public long fileRead, fileWritten;
    public long netRead, netWritten;
    public long transactions;

    public Map<Integer, List<String>> params;

    public long time() {
        return time;
    }

    public int method() {
        return method;
    }

    public int duration() {
        return duration;
    }

    public int suspendDuration() {
        return suspendDuration;
    }

    public int calls() {
        return calls;
    }

    public int traceFileIndex() {
        return traceFileIndex;
    }

    public int bufferOffset() {
        return bufferOffset;
    }

    public int recordIndex() {
        return recordIndex;
    }

    public int logsGenerated() {
        return logsGenerated;
    }

    public int logsWritten() {
        return logsWritten;
    }

    public String threadName() {
        return threadName;
    }

    public long cpuTime() {
        return cpuTime;
    }

    public long waitTime() {
        return waitTime;
    }

    public long memoryUsed() {
        return memoryUsed;
    }

    public Map<Integer, List<String>> params() {
        return params;
    }

    public long fileRead() {
        return fileRead;
    }

    public long fileWritten() {
        return fileWritten;
    }

    public long netRead() {
        return netRead;
    }

    public long netWritten() {
        return netWritten;
    }

    public long transactions() {
        return transactions;
    }

    public int queueWaitDuration() {
        return queueWaitDuration;
    }
}
