package com.netcracker.profiler.sax.call;

import com.netcracker.profiler.sax.io.DataInputStreamEx;
import com.netcracker.profiler.model.Call;

import java.io.IOException;
import java.util.BitSet;

public class CallDataReader_02 extends CallDataReader_01 {
    public void read(Call dst, DataInputStreamEx calls) throws IOException {
        super.read(dst, calls);
        dst.cpuTime = calls.readVarLong();
        dst.waitTime = calls.readVarLong();
        dst.memoryUsed = calls.readVarLong();
    }
}
