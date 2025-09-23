package com.netcracker.profiler.io.call;

import com.netcracker.profiler.dump.DataInputStreamEx;
import com.netcracker.profiler.io.Call;

import java.io.IOException;
import java.util.BitSet;

public class CallDataReader_03 extends CallDataReader_02 {
    public void read(Call dst, DataInputStreamEx calls, BitSet requiredIds) throws IOException {
        super.read(dst, calls, requiredIds);
        dst.fileRead = calls.readVarLong();
        dst.fileWritten = calls.readVarLong();
        dst.netRead = calls.readVarLong();
        dst.netWritten = calls.readVarLong();
    }
}
