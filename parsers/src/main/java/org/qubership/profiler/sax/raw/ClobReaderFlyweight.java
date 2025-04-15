package org.qubership.profiler.sax.raw;

import org.qubership.profiler.sax.values.ClobValue;


public interface ClobReaderFlyweight extends StrReader {
    void adaptTo(ClobValue clob);
}
