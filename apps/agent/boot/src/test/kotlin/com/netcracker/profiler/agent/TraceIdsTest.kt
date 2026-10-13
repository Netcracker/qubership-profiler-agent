package com.netcracker.profiler.agent

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

/**
 * A `traceparent` header is `version-traceid-parentid-flags` in lowercase hex, as W3C Trace Context
 * defines it. A `b3` header is `traceid-spanid[-sampling[-parentspanid]]`, as Zipkin's B3 single
 * format defines it. A header that breaks its format yields no ID at all, so a malformed value never
 * reaches the call as a trace ID.
 */
class TraceIdsTest {
    private val traceId = "4bf92f3577b34da6a3ce929d0e0e4736"
    private val parentId = "00f067aa0ba902b7"

    @Test
    fun `a version 00 header yields its trace ID and parent ID`() {
        val header = "00-$traceId-$parentId-01"
        assertEquals(traceId, TraceIds.traceparentTraceId(header))
        assertEquals(parentId, TraceIds.traceparentParentId(header))
    }

    @Test
    fun `whitespace around the header is ignored`() {
        assertEquals(traceId, TraceIds.traceparentTraceId("  00-$traceId-$parentId-00\t"))
    }

    @Test
    fun `a later version is read by its first four fields`() {
        val header = "01-$traceId-$parentId-01-extra"
        assertEquals(traceId, TraceIds.traceparentTraceId(header))
        assertEquals(parentId, TraceIds.traceparentParentId(header))
    }

    @Test
    fun `a missing header yields nothing`() {
        assertNull(TraceIds.traceparentTraceId(null))
        assertNull(TraceIds.traceparentParentId(null))
    }

    @ParameterizedTest
    @ValueSource(
        strings = [
            "",
            "00",
            // Version 00 has no fields after the flags
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra",
            // A later version separates an extra field with a dash
            "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01extra",
            "ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
            "00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e473-00f067aa0ba902b7-011",
            "00_4bf92f3577b34da6a3ce929d0e0e4736_00f067aa0ba902b7_01",
            "0g-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-0x",
        ]
    )
    fun `a malformed header yields nothing`(header: String) {
        assertNull(TraceIds.traceparentTraceId(header))
        assertNull(TraceIds.traceparentParentId(header))
    }

    @ParameterizedTest
    @ValueSource(
        strings = [
            "4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7",
            "4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-1",
            "4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-d",
            "4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-0-05e3ac9a4f6e3b90",
            " 4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-1 ",
        ]
    )
    fun `a b3 header yields its trace ID and span ID`(header: String) {
        assertEquals(traceId, TraceIds.b3TraceId(header))
        assertEquals(parentId, TraceIds.b3SpanId(header))
    }

    @Test
    fun `a b3 header keeps a 64-bit trace ID as it is`() {
        val header = "a3ce929d0e0e4736-$parentId-1"
        assertEquals("a3ce929d0e0e4736", TraceIds.b3TraceId(header))
        assertEquals(parentId, TraceIds.b3SpanId(header))
    }

    @Test
    fun `a missing b3 header yields nothing`() {
        assertNull(TraceIds.b3TraceId(null))
        assertNull(TraceIds.b3SpanId(null))
    }

    @ParameterizedTest
    @ValueSource(
        strings = [
            "",
            // A sampling decision alone names no trace
            "0",
            "d",
            "4bf92f3577b34da6a3ce929d0e0e4736",
            "4bf92f3577b34da6a3ce929d0e0e4736-",
            "4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b",
            "4bf92f3577b34da6a3ce929d0e0e473-00f067aa0ba902b7",
            "4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7",
            "00000000000000000000000000000000-00f067aa0ba902b7",
            "4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000",
            "4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-",
            "4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-2",
            "4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b71",
            "4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-1-",
            "4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-1-05e3ac9a4f6e3b9",
            "4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-1-05e3ac9a4f6e3b90-1",
            "4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-05e3ac9a4f6e3b90",
        ]
    )
    fun `a malformed b3 header yields nothing`(header: String) {
        assertNull(TraceIds.b3TraceId(header))
        assertNull(TraceIds.b3SpanId(header))
    }

    @Test
    fun `a numeric span ID is rendered as 16 hex characters`() {
        assertEquals("00f067aa0ba902b7", TraceIds.toSpanIdString(0x00f067aa0ba902b7L))
        assertEquals("0000000000000001", TraceIds.toSpanIdString(1L))
        assertEquals("ffffffffffffffff", TraceIds.toSpanIdString(-1L))
    }
}
