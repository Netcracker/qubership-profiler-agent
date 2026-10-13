package com.netcracker.profiler.tracing

import com.netcracker.profiler.agent.CallInfo
import com.netcracker.profiler.agent.Profiler
import com.netcracker.profiler.agent.TraceIds
import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.SpanContext
import io.opentelemetry.api.trace.TraceFlags
import io.opentelemetry.api.trace.TraceState
import io.opentelemetry.context.Context
import io.opentelemetry.sdk.trace.SdkTracerProvider
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test

/**
 * Runs under the agent and reads back the IDs stored for the profiled call: the ones the
 * `opentelemetry` plugin takes from the OpenTelemetry SDK, and the ones `TraceIds` takes from
 * propagation headers.
 */
class TraceIdsUnderAgentTest {
    private val tracer = SdkTracerProvider.builder().build().get("test")

    private fun <T> profiled(body: () -> T): T {
        Profiler.enter("OpenTelemetryTraceIdsTest.profiled")
        try {
            return body()
        } finally {
            Profiler.exit()
        }
    }

    private fun callInfo(): CallInfo = Profiler.getState().callInfo

    /**
     * Runs a profiled call that touches no tracer and returns its `CallInfo` once the call is over.
     *
     * A span that was current before the call started is recorded no later than the end of the
     * call, so the IDs are read afterward. The event keeps the call from being dropped as too
     * short to log.
     */
    private fun finishedCall(): CallInfo = profiled {
        Profiler.event("value", "test.param")
        callInfo()
    }

    @Test
    fun `a span created in a profiled call is recorded without being made current`() {
        profiled {
            val span = tracer.spanBuilder("created").startSpan()
            try {
                assertEquals(span.spanContext.traceId, callInfo().traceId, "traceId")
                assertEquals(span.spanContext.spanId, callInfo().spanId, "spanId")
                assertEquals(span.spanContext.traceId, callInfo().endToEndId, "endToEndId")
            } finally {
                span.end()
            }
        }
    }

    @Test
    fun `a span created elsewhere is recorded when it becomes current`() {
        val span = tracer.spanBuilder("handed over").startSpan()
        try {
            profiled {
                assertNotEquals(span.spanContext.spanId, callInfo().spanId, "spanId before the span is current")
                span.makeCurrent().use {
                    assertEquals(span.spanContext.traceId, callInfo().traceId, "traceId")
                    assertEquals(span.spanContext.spanId, callInfo().spanId, "spanId")
                }
            }
        } finally {
            span.end()
        }
    }

    @Test
    fun `a context propagated from another process is recorded as the parent`() {
        val remote = SpanContext.createFromRemoteParent(
            "4bf92f3577b34da6a3ce929d0e0e4736",
            "00f067aa0ba902b7",
            TraceFlags.getSampled(),
            TraceState.getDefault()
        )
        profiled {
            Context.root().with(Span.wrap(remote)).makeCurrent().use {
                assertEquals(remote.traceId, callInfo().traceId, "traceId")
                assertEquals(remote.spanId, callInfo().parentSpanId, "parentSpanId")
                assertNotEquals(remote.spanId, callInfo().spanId, "spanId")
            }
        }
    }

    @Test
    fun `a span continues a propagated context without the context being made current`() {
        val remote = SpanContext.createFromRemoteParent(
            "0af7651916cd43dd8448eb211c80319c",
            "b7ad6b7169203331",
            TraceFlags.getSampled(),
            TraceState.getDefault()
        )
        profiled {
            val span = tracer.spanBuilder("server").setParent(Context.root().with(Span.wrap(remote))).startSpan()
            try {
                assertEquals(remote.traceId, callInfo().traceId, "traceId")
                assertEquals(remote.spanId, callInfo().parentSpanId, "parentSpanId")
                assertEquals(span.spanContext.spanId, callInfo().spanId, "spanId")
            } finally {
                span.end()
            }
        }
    }

    @Test
    fun `a context made current before the call starts is recorded as the parent`() {
        val remote = SpanContext.createFromRemoteParent(
            "5b8aa5a2d2c872e8321cf37308d69df2",
            "051581bf3cb55c13",
            TraceFlags.getSampled(),
            TraceState.getDefault()
        )
        var call: CallInfo? = null
        Context.root().with(Span.wrap(remote)).wrap(Runnable { call = finishedCall() }).run()
        assertEquals(remote.traceId, call?.traceId, "traceId")
        assertEquals(remote.spanId, call?.parentSpanId, "parentSpanId")
        assertNull(call?.spanId, "spanId")
        assertEquals(remote.traceId, call?.endToEndId, "endToEndId")
    }

    @Test
    fun `a span made current before the call starts is recorded`() {
        val span = tracer.spanBuilder("handed over").startSpan()
        try {
            var call: CallInfo? = null
            Context.root().with(span).wrap(Runnable { call = finishedCall() }).run()
            assertEquals(span.spanContext.traceId, call?.traceId, "traceId")
            assertEquals(span.spanContext.spanId, call?.spanId, "spanId")
        } finally {
            span.end()
        }
    }

    @Test
    fun `every call that starts while a span stays current records it`() {
        val span = tracer.spanBuilder("handed over").startSpan()
        try {
            span.makeCurrent().use {
                finishedCall()
                assertEquals(span.spanContext.spanId, finishedCall().spanId, "spanId of the second call")
            }
        } finally {
            span.end()
        }
    }

    @Test
    fun `a span whose scope closed before the call started is not recorded`() {
        val span = tracer.spanBuilder("closed").startSpan()
        try {
            span.makeCurrent().close()
            val call = finishedCall()
            assertNull(call.traceId, "traceId")
            assertNull(call.spanId, "spanId")
        } finally {
            span.end()
        }
    }

    @Test
    fun `closing an inner scope before the call starts leaves the outer span`() {
        val outer = tracer.spanBuilder("outer").startSpan()
        val inner = tracer.spanBuilder("inner").startSpan()
        try {
            outer.makeCurrent().use {
                inner.makeCurrent().close()
                assertEquals(outer.spanContext.spanId, finishedCall().spanId, "spanId")
            }
        } finally {
            inner.end()
            outer.end()
        }
    }

    @Test
    fun `a span created in the call follows the span the call started under`() {
        val outer = tracer.spanBuilder("outer").startSpan()
        try {
            outer.makeCurrent().use {
                profiled {
                    val child = tracer.spanBuilder("child").startSpan()
                    try {
                        assertEquals(child.spanContext.spanId, callInfo().spanId, "spanId")
                        assertEquals(outer.spanContext.traceId, callInfo().traceId, "traceId")
                    } finally {
                        child.end()
                    }
                }
            }
        } finally {
            outer.end()
        }
    }

    @Test
    fun `a traceparent header is recorded as the trace and the parent`() {
        profiled {
            TraceIds.recordTraceparent("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
            assertEquals("0af7651916cd43dd8448eb211c80319c", callInfo().traceId, "traceId")
            assertEquals("b7ad6b7169203331", callInfo().parentSpanId, "parentSpanId")
        }
    }

    @Test
    fun `a b3 single header is recorded as the trace and the parent`() {
        profiled {
            TraceIds.recordB3("80f198ee56343ba864fe8b2a57d3eff7-e457b5a2e4d86bd1-1")
            assertEquals("80f198ee56343ba864fe8b2a57d3eff7", callInfo().traceId, "traceId")
            assertEquals("e457b5a2e4d86bd1", callInfo().parentSpanId, "parentSpanId")
        }
    }

    @Test
    fun `b3 multi headers are recorded as the trace and the parent`() {
        profiled {
            TraceIds.recordB3("463ac35c9f6413ad48485a3953bb6124", "a2fb4a1d1a96d312")
            assertEquals("463ac35c9f6413ad48485a3953bb6124", callInfo().traceId, "traceId")
            assertEquals("a2fb4a1d1a96d312", callInfo().parentSpanId, "parentSpanId")
        }
    }

    @Test
    fun `a malformed b3 trace ID is not recorded`() {
        profiled {
            TraceIds.recordB3("not-a-trace-id", "a2fb4a1d1a96d312")
            assertNotEquals("not-a-trace-id", callInfo().traceId, "traceId")
            assertNotEquals("a2fb4a1d1a96d312", callInfo().parentSpanId, "parentSpanId")
        }
    }
}
