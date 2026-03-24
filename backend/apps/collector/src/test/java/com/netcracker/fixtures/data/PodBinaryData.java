package com.netcracker.fixtures.data;

import com.netcracker.common.models.StreamType;

import java.io.IOException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public record PodBinaryData(Map<StreamType, BinaryFile> binaryData) {

    public static PodBinaryData TEST_SERVICE = new PodBinaryData(Map.ofEntries(
            Map.entry(StreamType.DICTIONARY, BinaryFile.of("binary/test-service.dictionary.bin", 331255)),
            Map.entry(StreamType.PARAMS, BinaryFile.of("binary/test-service.params.bin", 3704)),
            Map.entry(StreamType.TOP, BinaryFile.of("binary/test-service.top.txt", 9450)),
            Map.entry(StreamType.TD, BinaryFile.of("binary/test-service.td.txt", 51246)),
            Map.entry(StreamType.HEAP, BinaryFile.of("binary/test-service.heap.hprof.zip", 1456)),
            Map.entry(StreamType.GC, BinaryFile.of("binary/test-service.gc.0.bin", 17246)),
            Map.entry(StreamType.XML, BinaryFile.of("binary/test-service.xml.0.bin", 7541)),
            Map.entry(StreamType.SQL, BinaryFile.of("binary/test-service.sql.0.bin", 1825)),
            Map.entry(StreamType.CALLS, BinaryFile.of("binary/test-service.calls.0.bin", 28874)),
            Map.entry(StreamType.TRACE, BinaryFile.of("binary/test-service.traces.0.bin", 44290)),
            Map.entry(StreamType.SUSPEND, BinaryFile.of("binary/test-service.suspend.bin", 1275))
    ));

    public static PodBinaryData U5MIN_SERVICE = new PodBinaryData(Map.ofEntries(
            Map.entry(StreamType.DICTIONARY, BinaryFile.of("binary/u5min-service.dictionary.bin", 341866)),
            Map.entry(StreamType.PARAMS, BinaryFile.of("binary/u5min-service.params.bin", 3704)),
            Map.entry(StreamType.XML, BinaryFile.of("binary/u5min-service.xml.0.bin", 513753)),
            Map.entry(StreamType.SQL, BinaryFile.of("binary/u5min-service.sql.0.bin", 3253)),
            Map.entry(StreamType.CALLS, BinaryFile.of("binary/u5min-service.calls.0.bin", 309008)),
            Map.entry(StreamType.TRACE, BinaryFile.of("binary/u5min-service.traces.0.bin", 448239)),
            Map.entry(StreamType.SUSPEND, BinaryFile.of("binary/u5min-service.suspend.bin", 1275))
    ));

    public static PodBinaryData SMALL_SERVICE = new PodBinaryData(Map.ofEntries(
            Map.entry(StreamType.TOP, BinaryFile.of("binary/test-service.top.txt", 9450)),
            Map.entry(StreamType.TD, BinaryFile.of("binary/test-service.td.txt", 51246)),
            Map.entry(StreamType.HEAP, BinaryFile.of("binary/test-service.heap.hprof.zip", 1456))
    ));

    public byte[] getBytes(StreamType streamType) throws IOException {
        return get(streamType).getBytes();
    }

    public long getSize(StreamType streamType) throws IOException {
        return get(streamType).size();
    }

    public String getFilename(StreamType streamType) {
        return get(streamType).filename();
    }

    public BinaryFile get(StreamType streamType) {
        var res = binaryData.get(streamType);
        assertNotNull(res);
        return res;
    }

    public boolean has(StreamType streamType) {
        return binaryData.get(streamType) != null;
    }
}
