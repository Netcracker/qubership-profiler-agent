package org.qubership.profiler.io;

import java.io.InputStream;

public interface InputStreamProcessor {
    public boolean process(InputStream is, String name);
}
