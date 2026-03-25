package com.netcracker.cdt.ui.models;

import com.netcracker.common.models.meta.DictionaryIndex;
import com.netcracker.common.models.meta.ParamsModel;
import com.netcracker.utils.UnitTest;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static com.netcracker.utils.Utils.setOf;
import static org.junit.jupiter.api.Assertions.*;

@UnitTest
class DictionaryIndexTest {

    @Test
    void tags() {
        var idx = new DictionaryIndex();
        assertEquals(0, idx.getTags().size());
        assertEquals(0, idx.getParamInfo().size());

        idx.putDictionary(0, "tag0");
        idx.putDictionary(1, "tag1");
        idx.putDictionary(2, "tag2");

        assertEquals(setOf(0, 1, 2), idx.getIds());
        assertEquals(3, idx.getTags().size());
        assertEquals(List.of("tag0", "tag1", "tag2"), idx.getTags());

        idx.putDictionary(4, "tag4");
        assertEquals(setOf(0, 1, 2, 4), idx.getIds());
        assertEquals(5, idx.getTags().size());
        assertNull(idx.getTags().get(3));
        assertEquals("tag4", idx.getTags().get(4));
    }

    @Test
    void params() {
        var idx = new DictionaryIndex();
        assertEquals(0, idx.getTags().size());
        assertEquals(0, idx.getParamInfo().size());

        idx.putParameter(param("param1", 101, "method1"));
        idx.putParameter(param("param2", 102, "method2"));
        idx.putParameter(param("param3", 103, "method3"));

        assertEquals(0, idx.getTags().size());
        assertEquals(3, idx.getParamInfo().size());

        assertTrue(idx.getParamInfo().containsKey("param1"));
        assertTrue(idx.getParamInfo().containsKey("param2"));
        assertTrue(idx.getParamInfo().containsKey("param3"));
        assertNotNull(idx.getParamInfo().get("param1"));
        assertEquals(101, idx.getParamInfo().get("param1").paramOrder());

        assertFalse(idx.getParamInfo().containsKey("param4"));

    }

    @Test
    void cloning() {
        var idx = new DictionaryIndex();
        idx.putDictionary(0, "tag0");
        idx.putDictionary(1, "tag1");
        idx.putDictionary(2, "tag2");

        idx.putParameter(param("param1", 101, "method1"));
        idx.putParameter(param("param2", 102, "method2"));
        idx.putParameter(param("param3", 103, "method3"));

        assertEquals(3, idx.getTags().size());
        assertEquals(3, idx.getParamInfo().size());

        var idx2 = idx.clone();
        assertEquals(3, idx2.getTags().size());
        assertEquals(3, idx2.getParamInfo().size());

        assertEquals(setOf(0, 1, 2), idx2.getIds());
        assertTrue(idx2.getParamInfo().containsKey("param1"));
        assertTrue(idx2.getParamInfo().containsKey("param2"));
        assertTrue(idx2.getParamInfo().containsKey("param3"));
        assertNotNull(idx2.getParamInfo().get("param1"));
        assertEquals(101, idx2.getParamInfo().get("param1").paramOrder());

    }

    @Test
    void merge() {
        var idx1 = new DictionaryIndex();
        idx1.putDictionary(0, "tag10");
        idx1.putDictionary(1, "tag11");

        idx1.putParameter(param("param11", 101, "method1"));
        idx1.putParameter(param("param12", 102, "method2"));
        idx1.putParameter(param("param13", 103, "method3"));

        assertEquals(2, idx1.getTags().size());
        assertEquals(3, idx1.getParamInfo().size());


        var idx2 = new DictionaryIndex();
        idx2.putDictionary(0, "tag20");
        idx2.putDictionary(1, "tag21");
        idx2.putDictionary(2, "tag22");

        idx2.putParameter(param("param21", 101, "method1"));
        idx2.putParameter(param("param22", 102, "method2"));

        assertEquals(3, idx2.getTags().size());
        assertEquals(2, idx2.getParamInfo().size());

        assertEquals(Map.of(), idx1.mergeForRemap(idx1));

        var remap = idx1.mergeForRemap(idx2);
        assertEquals(5, idx1.getTags().size());
        assertEquals(5, idx1.getParamInfo().size());

        assertTrue(idx1.getParamInfo().containsKey("param11"));
        assertTrue(idx1.getParamInfo().containsKey("param12"));
        assertTrue(idx1.getParamInfo().containsKey("param13"));
        assertTrue(idx1.getParamInfo().containsKey("param21"));
        assertTrue(idx1.getParamInfo().containsKey("param21"));

        assertEquals(Map.of(0, 2, 1, 3, 2, 4), remap);
        assertEquals(setOf(0, 1, 2, 3, 4), idx1.getIds());
        assertEquals(5, idx1.getTags().size());
        assertEquals(List.of("tag10", "tag11", "tag20", "tag21", "tag22"), idx1.getTags());
    }

    ParamsModel param(String paramName, int paramOrder, String signature) {
        return new ParamsModel(null, paramName, true, true, paramOrder, signature);
    }

}