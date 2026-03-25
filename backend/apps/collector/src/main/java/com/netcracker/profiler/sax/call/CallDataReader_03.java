package com.netcracker.profiler.sax.call;

import com.netcracker.profiler.sax.io.DataInputStreamEx;
import com.netcracker.profiler.model.Call;

import java.io.IOException;
import java.util.BitSet;

public class CallDataReader_03 extends CallDataReader_02 {
    public void read(Call dst, DataInputStreamEx calls) throws IOException {
        super.read(dst, calls);
        dst.fileRead = calls.readVarLong();
        dst.fileWritten = calls.readVarLong();
        dst.netRead = calls.readVarLong();
        dst.netWritten = calls.readVarLong();
    }
}
