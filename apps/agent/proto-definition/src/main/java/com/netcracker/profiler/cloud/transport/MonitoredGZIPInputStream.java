package com.netcracker.profiler.cloud.transport;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

public class MonitoredGZIPInputStream extends GZIPInputStream {
    public MonitoredGZIPInputStream(InputStream in, int size) throws IOException {
        super(in, size);
    }

    public MonitoredGZIPInputStream(InputStream in) throws IOException {
        super(in);
    }

    @Override
    public int available() throws IOException {
        //if inf doesn't need input, there is some data in buffer
        return super.available() + (inf.needsInput()?0:1);
    }

//    public boolean hasDataInBuffer(){
//        return !inf.needsInput();
//    }
}
