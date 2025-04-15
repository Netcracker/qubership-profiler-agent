package org.qubership.profiler.test.dump;

import org.qubership.profiler.dump.FlushableGZIPOutputStream;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.*;
import java.util.zip.GZIPInputStream;

public class FlushableGzipTest {
    @Test
    public void flushStoresIntermediateData() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        @SuppressWarnings("resource")
        OutputStream gzip = new FlushableGZIPOutputStream(baos);

        final byte[] source = {1, 1, 1, 1, 1, 2, 2, 2, 2, 2};

        gzip.write(source, 0, source.length);
        gzip.flush();

        final byte[] compressed = baos.toByteArray();
        InputStream in = new GZIPInputStream(new ByteArrayInputStream(compressed));
        final byte[] decompressed = new byte[source.length];

        int bytesRead = in.read(decompressed);

        Assert.assertEquals(bytesRead, source.length);
        Assert.assertEquals(decompressed, source);
    }
}
