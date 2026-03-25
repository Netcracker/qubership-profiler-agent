package com.netcracker.profiler.sax.call;

import com.netcracker.profiler.sax.io.DataInputStreamEx;
import com.netcracker.profiler.model.Call;

import java.io.IOException;

public interface CallDataReader {
    void read(Call dst, DataInputStreamEx calls) throws IOException;

    void readParams(Call dst, DataInputStreamEx calls) throws IOException;

    void skipParams(Call dst, DataInputStreamEx calls) throws IOException;
}
