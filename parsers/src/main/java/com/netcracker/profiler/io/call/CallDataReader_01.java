package com.netcracker.profiler.io.call;

import com.netcracker.profiler.dump.DataInputStreamEx;
import com.netcracker.profiler.io.Call;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;

public class CallDataReader_01 extends CallDataReaderBase {
    protected ArrayList<String> threadNames = new ArrayList<String>();

    public void read(Call dst, DataInputStreamEx calls, BitSet requiredIds) throws IOException {
        dst.time = calls.readVarIntZigZag();
        dst.method = calls.readVarInt();
        requiredIds.set(dst.method);
        dst.duration = calls.readVarInt();
        dst.calls = calls.readVarInt();
        int threadIndex = calls.readVarInt();
        if (threadNames != null) {
            if (threadIndex == threadNames.size())
                threadNames.add(calls.readString());
            try {
                //in case of zip errors thread index may be larger than number of threads
                if(threadNames.size() > threadIndex) {
                    dst.threadName = threadNames.get(threadIndex);
                } else {
                    dst.threadName = "unknown # " + threadIndex;
                }
            } catch(IndexOutOfBoundsException e) {
                IOException exception = new IOException("Unable to decode call since referenced thread index is out of bounds " + dst);
                exception.initCause(e);
                throw exception;
            }
        }

        dst.logsWritten = calls.readVarInt();
        dst.logsGenerated = calls.readVarInt() + dst.logsWritten;
        dst.traceFileIndex = calls.readVarInt();
        dst.bufferOffset = calls.readVarInt();
        dst.recordIndex = calls.readVarInt();
    }
}
