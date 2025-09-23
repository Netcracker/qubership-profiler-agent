package com.netcracker.profiler.sax.raw;

import com.netcracker.profiler.sax.values.ClobValue;


public interface ClobReaderFlyweight extends StrReader {
    void adaptTo(ClobValue clob);
}
