package com.netcracker.profiler.io.call;

import com.netcracker.profiler.dump.DataInputStreamEx;
import com.netcracker.profiler.io.Call;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class CallDataReader_04 extends CallDataReader_03 {
    public void read(Call dst, DataInputStreamEx calls, BitSet requiredIds) throws IOException {
        super.read(dst, calls, requiredIds);
        dst.transactions = calls.readVarInt();
        dst.queueWaitDuration = calls.readVarInt();
//        boolean isReactor = calls.read() == 1;
//        dst.isReactor = isReactor;
//        if (isReactor) {
//            if (calls.read() == 1) {
//                dst.reactorParentId = calls.readLong();
//            }
//        }
//        dst.reactorFileIndex = calls.readVarInt();
//        dst.reactorBufferOffset = calls.readVarInt();
    }

    @Override
    public void postCompute(ArrayList<Call> result, List<String> tags, BitSet requredIds) {
    }
}
