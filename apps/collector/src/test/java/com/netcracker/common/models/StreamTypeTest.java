package com.netcracker.common.models;

import com.netcracker.utils.UnitTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@UnitTest
class StreamTypeTest {

    @Test
    void allStreams() {
        assertEquals(11, StreamType.allStreams().size());
        assertTrue(StreamType.allStreams().contains("xml"));
        assertFalse(StreamType.allStreams().contains("test"));
    }

    @Test
    void byName() {
        assertNull(StreamType.byName("test"));

        StreamType stream = StreamType.byName("xml");
        assertNotNull(stream);
        assertEquals("xml", stream.getName());
        assertEquals("xml", stream.getFileExtension());
        assertTrue(stream.isRotationRequired());
        assertEquals("xml", stream.toString());

        stream = StreamType.byName("params");
        assertNotNull(stream);
        assertEquals("params", stream.getName());
        assertEquals("", stream.getFileExtension());
        assertFalse(stream.isRotationRequired());
        assertEquals("params", stream.toString());

        stream = StreamType.byName("dictionary");
        assertNotNull(stream);
        assertEquals("dictionary", stream.getName());
        assertEquals("", stream.getFileExtension());
        assertFalse(stream.isRotationRequired());
        assertEquals("dictionary", stream.toString());

        stream = StreamType.byName("calls");
        assertNotNull(stream);
        assertEquals("calls", stream.getName());
        assertEquals("", stream.getFileExtension());
        assertTrue(stream.isRotationRequired());
        assertEquals("calls", stream.toString());

        stream = StreamType.byName("td");
        assertNotNull(stream);
        assertEquals("td", stream.getName());
        assertEquals("td.txt", stream.getFileExtension());
        assertTrue(stream.isRotationRequired());
        assertEquals("td", stream.toString());

        stream = StreamType.byName("heap");
        assertNotNull(stream);
        assertEquals("heap", stream.getName());
        assertEquals("hprof.zip", stream.getFileExtension());
        assertTrue(stream.isRotationRequired());
        assertEquals("heap", stream.toString());
    }
}