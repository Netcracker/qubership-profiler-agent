package com.netcracker.profiler.test.dump;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.netcracker.profiler.dump.FlushableGZIPOutputStream;

import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.Arrays;
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

        assertEquals(Arrays.toString(source), Arrays.toString(decompressed), "Contents should be intact after compress and decompress");
        assertEquals(source.length, bytesRead, "Decompressed length should be the same as the original");
    }
}
