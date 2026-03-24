package com.netcracker.profiler.model;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CallRowIdTest {

    Map<String, Object> params = Map.of("f[_7]", List.of("fileName1"), "f[_8]", List.of("fileName2"));

    @Test
    void parse() {
        // q ::= fullAddress _ traceFileIndex _ bufferOffset _ recordIndex _ reactorFileIndex _ reactorBufferOffset
        var q1 = "7_1_903060_2_0_0";
        var q2 = "8_0_909060_7_0_0";

        var id1 = CallRowId.parse(q1, params);
        assertEquals("fileName1", id1.file());
        assertEquals(1, id1.treeRow().traceFileIndex);
        assertEquals(903060, id1.treeRow().bufferOffset);
        assertEquals(2, id1.treeRow().recordIndex);
        assertEquals(q1, id1.treeRow().fullRowId);

        assertEquals("CallRowId[file=fileName1, treeRow=TreeRowid{traceFileIndex=1, bufferOffset=903060, recordIndex=2, fullRowId='7_1_903060_2_0_0', folderId=7}]", id1.toString());

        var id2 = CallRowId.parse(q2, params);
        assertEquals("fileName2", id2.file());
        assertEquals(0, id2.treeRow().traceFileIndex);
        assertEquals(909060, id2.treeRow().bufferOffset);
        assertEquals(7, id2.treeRow().recordIndex);
        assertEquals(q2, id2.treeRow().fullRowId);
    }

    @Test
    void sort() {
        // sort order: file -> traceFileIndex -> bufferOffset -> recordIndex

        var id1 = CallRowId.parse("7_1_903060_2_0_0", params);
        var id2 = CallRowId.parse("7_2_904060_0_0_0", params);
        var q3 = "8_1_909060_0_0_0";
        var id3 = CallRowId.parse(q3, params);
        var id33 = CallRowId.parse(q3, params);

        assertEquals(-1, id1.compareTo(id2));
        assertEquals(-1, id2.compareTo(id3));
        assertEquals(-1, id1.compareTo(id3));

        assertTrue(id3.equals(id33));
        assertEquals(0, id3.compareTo(id33));

        var list = new ArrayList<>(List.of(id33, id2, id1, id3));
        Collections.sort(list);
        assertEquals(List.of(id1, id2, id3, id3), list);

    }
}