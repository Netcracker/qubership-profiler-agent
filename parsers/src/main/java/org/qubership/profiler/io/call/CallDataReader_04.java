package org.qubership.profiler.io.call;

import org.qubership.profiler.dump.DataInputStreamEx;
import org.qubership.profiler.io.Call;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class CallDataReader_04 extends CallDataReader_03 {
    public void read(Call dst, DataInputStreamEx calls, BitSet requiredIds) throws IOException {
        super.read(dst, calls, requiredIds);
        dst.transactions = calls.readVarInt();
        dst.queueWaitDuration = calls.readVarInt();
    }

    @Override
    public void postCompute(ArrayList<Call> result, List<String> tags, BitSet requredIds) {
    }
}
