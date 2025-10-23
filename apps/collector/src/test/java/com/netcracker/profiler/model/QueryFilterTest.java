package com.netcracker.profiler.model;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class QueryFilterTest {

    @Test
    void testEmptyFilterStringReturnsEmptyCondition() {
        QueryFilter result = QueryFilter.parseQueryFilter("");
        assertTrue(result.get_included().isEmpty());
        assertTrue(result.get_excluded().isEmpty());
    }

    @Test
    void testSingleIncludeKey() {
        QueryFilter result = QueryFilter.parseQueryFilter("+request.id \"abc123\"");
        Map<String, List<String>> included = result.get_included();

        assertTrue(included.containsKey("request.id"));
        assertEquals(List.of("abc123"), included.get("request.id"));
        assertTrue(result.get_excluded().isEmpty());
    }

    @Test
    void testSingleExcludeKey() {
        QueryFilter result = QueryFilter.parseQueryFilter("-trace.id \"t5678\"");
        Map<String, List<String>> excluded = result.get_excluded();

        assertTrue(excluded.containsKey("trace.id"));
        assertEquals(List.of("t5678"), excluded.get("trace.id"));
        assertTrue(result.get_included().isEmpty());
    }

    @Test
    void testMultipleIncludesAndExcludes() {
        QueryFilter result = QueryFilter.parseQueryFilter("+request.id \"abc123\" AND -trace.id \"t5678\"");
        Map<String, List<String>> included = result.get_included();
        Map<String, List<String>> excluded = result.get_excluded();

        assertEquals(List.of("abc123"), included.get("request.id"));
        assertEquals(List.of("t5678"), excluded.get("trace.id"));
    }

    @Test
    void testLogicalNotNegatesInclude() {
        QueryFilter result = QueryFilter.parseQueryFilter("NOT +jms.replyto \"queue1\"");
        Map<String, List<String>> excluded = result.get_excluded();

        assertTrue(excluded.containsKey("jms.replyto"));
        assertEquals(List.of("queue1"), excluded.get("jms.replyto"));
        assertTrue(result.get_included().isEmpty());
    }

    @Test
    void testLogicalNotNegatesExclude() {
        QueryFilter result = QueryFilter.parseQueryFilter("NOT -jms.replyto \"topic1\"");
        Map<String, List<String>> included = result.get_included();

        assertTrue(included.containsKey("jms.replyto"));
        assertEquals(List.of("topic1"), included.get("jms.replyto"));
        assertTrue(result.get_excluded().isEmpty());
    }

    @Test
    void testMultipleValuesForSameKey() {
        QueryFilter result = QueryFilter.parseQueryFilter("+trace.id \"x1\" +trace.id \"x2\"");
        Map<String, List<String>> included = result.get_included();

        assertTrue(included.containsKey("trace.id"));
        assertEquals(List.of("x1", "x2"), included.get("trace.id"));
    }

    @Test
    void testQuotesAreStripped() {
        QueryFilter result = QueryFilter.parseQueryFilter("+request.id \"my-id\"");
        assertEquals(List.of("my-id"), result.get_included().get("request.id"));
    }

    @Test
    void testToStringFormat() {
        QueryFilter result = QueryFilter.parseQueryFilter("+request.id \"abc\" -jms.replyto \"reply1\"");
        String expected = "QueryFilter{included={request.id=[abc]}, excluded={jms.replyto=[reply1]}}";
        assertEquals(expected, result.toString());
    }
}
