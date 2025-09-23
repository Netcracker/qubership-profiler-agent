package com.netcracker.profiler.io;

import com.netcracker.profiler.agent.Bootstrap;
import com.netcracker.profiler.agent.DumperPlugin;
import com.netcracker.profiler.agent.DumperPlugin_03;
import com.netcracker.profiler.agent.InflightCall;

import java.io.File;
import java.util.Arrays;
import java.util.List;

public class ParamReaderFromMemory_01 extends ParamReaderFromMemory {
    public ParamReaderFromMemory_01(File root) {
        super(root);
        DumperPlugin_03.class.getName();
    }

    @Override
    public Object[] getInflightCalls() {
        final DumperPlugin_03 dumper = (DumperPlugin_03) Bootstrap.getPlugin(DumperPlugin.class);
        if (dumper == null) return null;
        final Object[] inflightCalls = dumper.getInflightCalls();
        if (inflightCalls == null) return null;
        inflightCalls[1] = convertCalls((List<InflightCall>) inflightCalls[1]);
        return inflightCalls;
    }

    protected Call convert(InflightCall call) {
        Call dst = new Call();
        dst.time = call.time();
        dst.method = call.method();
        dst.duration = call.duration();
        dst.suspendDuration = call.suspendDuration();
        dst.calls = call.calls();
        dst.traceFileIndex = call.traceFileIndex();
        dst.bufferOffset = call.bufferOffset();
        dst.recordIndex = call.recordIndex();
        dst.logsGenerated = call.logsGenerated();
        dst.logsWritten = call.logsWritten();
        dst.threadName = call.threadName();
        dst.cpuTime = call.cpuTime();
        dst.waitTime = call.waitTime();
        dst.memoryUsed = call.memoryUsed();
        dst.params = call.params();
        return dst;
    }

    private List<Call> convertCalls(List<InflightCall> list) {
        Call[] res = new Call[list.size()];
        for (int i = 0, listSize = list.size(); i < listSize; i++)
            res[i] = convert(list.get(i));
        return Arrays.asList(res);
    }
}
