package org.qubership.profiler.dump;

import org.qubership.profiler.agent.CallInfo;

import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.hash.THashSet;

public class ThreadState {
    public CallInfo callInfo;
    public int time, calls;
    public int traceFileIndex;
    public int bufferOffset;
    public int recordIndex;
    public int method;
    public long prevCpuTime;
    public long prevWaitTime;
    public long prevMemoryUsed;
    public long prevFileRead, prevFileWritten;
    public long prevNetRead, prevNetWritten;
    public long prevTransactions;

    public TIntObjectHashMap<THashSet<String>> params = new TIntObjectHashMap<THashSet<String>>();

    public void saveThreadCounters(CallInfo callInfo) {
        if (callInfo == null) return;
        this.callInfo = callInfo.next; // this will be not null
        prevCpuTime = callInfo.cpuTime;
        prevWaitTime = callInfo.waitTime;
        prevMemoryUsed = callInfo.memoryUsed;
        prevFileRead = callInfo.fileRead;
        prevFileWritten = callInfo.fileWritten;
        prevNetRead = callInfo.netRead;
        prevNetWritten = callInfo.netWritten;
        prevTransactions = callInfo.transactions;
    }
}
