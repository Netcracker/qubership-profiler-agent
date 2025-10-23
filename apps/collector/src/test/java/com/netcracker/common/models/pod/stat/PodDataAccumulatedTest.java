package com.netcracker.common.models.pod.stat;

import com.netcracker.common.models.StreamType;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PodDataAccumulatedTest {

    @Test
    public void testStatFromDB() {
        var original = Map.of("dictionary", 10L, "params", 20L);
        var compressed = Map.of("dictionary", 30L, "params", 40L);
        var data = PodDataAccumulated.fromDb( original, compressed);

        var blobSize = data.map().get(StreamType.DICTIONARY);
        assertEquals(10L, blobSize.original);
        assertEquals(30L, blobSize.compressed);

        blobSize = data.map().get(StreamType.PARAMS);
        assertEquals(20L, blobSize.original);
        assertEquals(40L, blobSize.compressed);
    }
}
