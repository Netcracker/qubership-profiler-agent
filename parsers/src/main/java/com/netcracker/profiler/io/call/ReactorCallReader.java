package com.netcracker.profiler.io.call;

import com.netcracker.profiler.dump.DataInputStreamEx;
import com.netcracker.profiler.io.Call;

import java.io.IOException;

public interface ReactorCallReader {
     void read(Call dst, DataInputStreamEx calls) throws IOException;
}
