package com.netcracker.profiler;

import java.io.Closeable;
import java.io.IOException;

public interface IDumper extends Closeable {
    public void initialize() throws IOException;
    public void dumpLoop() throws InterruptedException, IOException;
}
