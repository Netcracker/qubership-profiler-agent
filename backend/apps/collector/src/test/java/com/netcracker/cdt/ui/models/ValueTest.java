package com.netcracker.cdt.ui.models;

import com.netcracker.common.models.StreamType;
import com.netcracker.common.models.meta.Value;
import com.netcracker.profiler.sax.io.DataInputStreamEx;
import com.netcracker.utils.UnitTest;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.netcracker.utils.Utils.byteStream;
import static org.junit.jupiter.api.Assertions.*;

@UnitTest
class ValueTest {

    @Test
    void str() {
        var s0 = Value.str(null);
        assertTrue(s0.isEmpty());
        var sE = Value.str("");
        assertTrue(sE.isEmpty());
        var s1 = Value.str("t1");
        assertFalse(s1.isEmpty());
        assertEquals("t1", s1.get());
        assertEquals("t1", s1.value());
        assertEquals("t1", s1.toString());
    }

    @Test
    void clob() {
        var c1 = Value.clob("pod3", StreamType.SQL, 1, 405);
        var c2 = Value.clob("pod9", StreamType.XML, 4, 0);

        assertTrue(c1.isEmpty());
        assertEquals("pod3", c1.id().podReference());
        assertEquals(StreamType.SQL, c1.id().clobType());
        assertEquals(1, c1.id().fileIndex());
        assertEquals(405, c1.id().offset());
        assertEquals("Clob(sql: fileIndex=1, offset=405, pod=pod3)", c1.toString());

        assertTrue(c2.isEmpty());
        assertEquals("pod9", c2.id().podReference());
        assertEquals(StreamType.XML, c2.id().clobType());
        assertEquals(4, c2.id().fileIndex());
        assertEquals(0, c2.id().offset());
        assertEquals("Clob(xml: fileIndex=4, offset=0, pod=pod9)", c2.toString());

        c1.set("TEST");
        assertFalse(c1.isEmpty());
        // don't show value in toString:
        assertEquals("Clob(sql: fileIndex=1, offset=405, pod=pod3)", c1.toString());
        assertEquals("Clob(sql: fileIndex=1, offset=405, pod=pod3)", c1.id().toString());

    }

    @Test
    void clobValue() throws IOException {
        var c1 = Value.clob("pod3", StreamType.SQL, 1, 405);
        assertTrue(c1.isEmpty());

        c1.set("TEST");
        assertFalse(c1.isEmpty());
        assertEquals("TEST", c1.get());

        var c2 = Value.clob("pod9", StreamType.XML, 4, 3);
        assertTrue(c2.isEmpty());

        c2.readFrom(testStream(), 10);
        assertFalse(c2.isEmpty());
        assertEquals("cal.lac", c2.get());

        c2.readFrom(testStream(), 2);
        assertFalse(c2.isEmpty());
        assertEquals("ca", c2.get());
    }

    @Test
    void clobSort() {
        // sort order: clobType -> fileIndex -> offset -> podReference

        var c11 = Value.clob("pod1", StreamType.SQL, 1, 0);
        var c12 = Value.clob("pod1", StreamType.SQL, 1, 104);
        var c2 = Value.clob("pod1", StreamType.SQL, 2, 0);
        var c3 = Value.clob("pod1", StreamType.XML, 2, 0);
        var c4 = Value.clob("pod3", StreamType.XML, 1, 405);
        var c4c = Value.clob("pod3", StreamType.XML, 1, 405);

        assertNotEquals(c4, c11);
        assertEquals(c4, c4c);

        assertEquals(-1, c11.compareTo(c12));
        assertEquals(-1, c12.compareTo(c2));
        assertEquals(-1, c2.compareTo(c3));
        assertEquals(1, c3.compareTo(c4));

        var list = new ArrayList<>(List.of(c11, c3, c4, c2, c12));
        Collections.sort(list);

        assertEquals(List.of(c11, c12, c2, c4, c3), list);

    }

    static DataInputStreamEx testStream() {
        return byteStream(0x99, 0x99, 0x99, 0x07, 0, 0x63, 0, 0x61, 0, 0x6c, 0, 0x2e, 0, 0x6c, 0, 0x61, 0, 0x63, 0, 0x63, 1);
    }
}