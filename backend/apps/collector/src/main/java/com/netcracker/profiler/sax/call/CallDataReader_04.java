package com.netcracker.profiler.sax.call;

import com.netcracker.profiler.sax.io.DataInputStreamEx;
import com.netcracker.profiler.model.Call;

import java.io.IOException;

public class CallDataReader_04 extends CallDataReader_03 {
    public void read(Call dst, DataInputStreamEx calls) throws IOException {
        super.read(dst, calls);
        dst.transactions = calls.readVarInt();
        dst.queueWaitDuration = calls.readVarInt();
    }

}
