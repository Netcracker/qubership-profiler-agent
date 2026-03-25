package com.netcracker.cdt.ui.models;

import com.netcracker.common.models.StreamType;
import com.netcracker.common.models.meta.ClobIndex;
import com.netcracker.common.models.meta.Value;
import com.netcracker.profiler.sax.io.DataInputStreamEx;
import com.netcracker.utils.UnitTest;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static com.netcracker.utils.Utils.byteStream;
import static org.junit.jupiter.api.Assertions.*;

@UnitTest
class ClobIndexTest {

    @Test
    void workflow() throws IOException {
        var idx = new ClobIndex(10);

        var c1 = clob(1, false);
        idx.getOrDefault(c1.id(), c1);
        assertEquals(1, idx.uniqToLoad().size());
        var c2 = clob(2, true);
        idx.getOrDefault(c2.id(), c2);
        assertEquals(2, idx.uniqToLoad().size());

        var c3 = clob(3, false);
        idx.getOrDefault(c3.id(), c3);
        assertEquals(3, idx.uniqToLoad().size());
        idx.load(c3, testStream());
        assertEquals(2, idx.uniqToLoad().size());

        var c3c = clob(3, false);
        c3c.set("COPY");
        idx.getOrDefault(c3c.id(), c3c);
        assertEquals(2, idx.uniqToLoad().size());
        assertTrue(idx.has(c3c.id()));
        assertEquals("cal.lac", idx.text(c3c.id())); // c3, not 'COPY' from c3c

        assertEquals(1, idx.getClobs().size());
        assertEquals(List.of(c3), idx.getClobs());
        assertEquals(List.of(c3c), idx.getClobs()); // compare by id(), not reference
        assertNotEquals(List.of(c1), idx.getClobs());

    }

    @Test
    void maxLength() throws IOException {
        var idx = new ClobIndex(2);

        var c = clob(3, false);
        idx.getOrDefault(c.id(), c);
        assertEquals(1, idx.uniqToLoad().size());
        idx.load(c, testStream());
        assertEquals(0, idx.uniqToLoad().size());

        assertTrue(idx.has(c.id()));
        assertEquals("ca", idx.text(c.id()));

    }

    @Test
    void merge() throws IOException {
        var idx1 = new ClobIndex(6);

        var c1 = clob(3, true);
        idx1.getOrDefault(c1.id(), c1);
        idx1.load(c1, testStream());
        assertEquals(List.of(c1), idx1.getClobs());

        var c2 = clob(5, false);
        idx1.getOrDefault(c2.id(), c2);
        idx1.load(c2, testStream());
        assertEquals(List.of(c1, c2), idx1.getClobs());

        var idx2 = new ClobIndex(6);

        var c3 = clob(3, false); // != c1
        idx2.getOrDefault(c3.id(), c3);
        idx2.load(c3, testStream());
        assertEquals(List.of(c3), idx2.getClobs());

        var c4 = clob(3, true); // == c1
        idx2.getOrDefault(c4.id(), c4);
        idx2.load(c4, testStream());
        assertEquals(List.of(c3, c4), idx2.getClobs());

        assertFalse(idx2.has(c2.id()));
        assertNull(idx2.text(c2.id()));

        idx2.merge(idx1);

        assertTrue(idx2.has(c2.id()));
        assertEquals("cal.la", idx2.text(c2.id()));
        assertEquals(List.of(c3, c4, c2), idx2.getClobs());
    }

    static Value.Clob clob(int fileIdx, boolean xml) {
        return Value.clob("pod1", xml ? StreamType.XML : StreamType.SQL, fileIdx, 3);
    }

    static DataInputStreamEx testStream() {
        return byteStream(0x99, 0x99, 0x99, 0x07, 0, 0x63, 0, 0x61, 0, 0x6c, 0, 0x2e, 0, 0x6c, 0, 0x61, 0, 0x63, 0, 0x63, 1);
    }
}