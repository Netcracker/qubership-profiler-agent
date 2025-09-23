package com.netcracker.profiler.io;

import java.util.List;
import java.util.Map;

public class Call {
    public String reactorChainId;
    public long time;
    public long cpuTime;
    public long waitTime;
    public long memoryUsed;
    public int method;
    public int duration;
    public long nonBlocking;
    public int queueWaitDuration;
    public int suspendDuration;
    public int calls;
    public int traceFileIndex;
    public int bufferOffset;
    public int reactorFileIndex;
    public int reactorBufferOffset;
    public int recordIndex;
    public long transactions;
    public int logsGenerated, logsWritten;
    public long fileRead, fileWritten;
    public long netRead, netWritten;
    public String threadName;
    public String callsStreamIndex;

    public Map<Integer, List<String>> params;

    public void setSuspendDuration(int suspendDuration) {
        this.suspendDuration = suspendDuration;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Call)) return false;

        Call call = (Call) o;

        if (bufferOffset != call.bufferOffset) return false;
        if (recordIndex != call.recordIndex) return false;
        if (traceFileIndex != call.traceFileIndex) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = traceFileIndex;
        result = 31 * result + bufferOffset;
        result = 31 * result + recordIndex;
        return result;
    }

    @Override
    public String toString() {
        return "Call{" +
                "time=" + time +
                ", cpuTime=" + cpuTime +
                ", waitTime=" + waitTime +
                ", memoryUsed=" + memoryUsed +
                ", method=" + method +
                ", duration=" + duration +
                ", queueWaitDuration=" + queueWaitDuration +
                ", suspendDuration=" + suspendDuration +
                ", calls=" + calls +
                ", traceFileIndex=" + traceFileIndex +
                ", bufferOffset=" + bufferOffset +
                ", recordIndex=" + recordIndex +
                ", transactions=" + transactions +
                ", logsGenerated=" + logsGenerated +
                ", logsWritten=" + logsWritten +
                ", fileRead=" + fileRead +
                ", fileWritten=" + fileWritten +
                ", netRead=" + netRead +
                ", netWritten=" + netWritten +
                ", threadName='" + threadName + '\'' +
                ", params=" + params +
                '}';
    }
}
