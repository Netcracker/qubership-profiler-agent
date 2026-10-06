package com.netcracker.profiler.io.call;

import com.netcracker.profiler.dump.DataInputStreamEx;
import com.netcracker.profiler.io.Call;
import com.netcracker.profiler.tags.Dictionary;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;

public interface CallDataReader {
    public void read(Call dst, DataInputStreamEx calls, BitSet requiredIds) throws IOException;

    public void readParams(Call dst, DataInputStreamEx calls, BitSet requiredIds) throws IOException;

    public void skipParams(Call dst, DataInputStreamEx calls) throws IOException;

    public void postCompute(ArrayList<Call> result, Dictionary tags, BitSet requredIds);
}
