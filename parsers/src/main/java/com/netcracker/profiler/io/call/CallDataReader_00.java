package com.netcracker.profiler.io.call;

import com.netcracker.profiler.dump.DataInputStreamEx;
import com.netcracker.profiler.io.Call;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class CallDataReader_00 extends CallDataReaderBase {
    public void read(Call dst, DataInputStreamEx calls, BitSet requiredIds) throws IOException {
        dst.time = calls.readVarIntZigZag();
        dst.method = calls.readVarInt();
        requiredIds.set(dst.method);
        dst.duration = calls.readVarInt();
        dst.calls = calls.readVarInt();
        dst.traceFileIndex = calls.readVarInt();
        dst.bufferOffset = calls.readVarInt();
        dst.recordIndex = calls.readVarInt();
    }

    @Override
    public void postCompute(ArrayList<Call> result, List<String> tags, BitSet requredIds) {
        Integer j2eeXid = null;
        for (int i = -1; (i = requredIds.nextSetBit(i + 1)) >= 0; ) {
            String tag = tags.get(i);
            if ("j2ee.xid".equals(tag)) {
                j2eeXid = i;
                break;
            }
        }
        if (j2eeXid == null) return;
        for (Call call : result) {
            if (call.params != null) {
                final List<String> txs = call.params.get(j2eeXid);
                if (txs != null)
                    call.transactions = txs.size();
            }
        }
    }
}
