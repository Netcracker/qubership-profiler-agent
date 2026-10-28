package com.netcracker.profiler.tracing

import com.netcracker.profiler.agent.CallInfo
import com.netcracker.profiler.agent.Profiler
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.SpanContext
import io.opentelemetry.api.trace.TraceFlags
import io.opentelemetry.api.trace.TraceState
import io.opentelemetry.context.Context
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

/**
 * Runs with the profiler and the OpenTelemetry Java agent attached together, and reads back the IDs
 * stored for the profiled call.
 *
 * The tracer comes from the agent, so every span here is the agent's own SDK span, built on its
 * relocated API. The test reaches it through the application's OpenTelemetry API, which the agent
 * routes to its own.
 */
@Tag("otel-javaagent")
class OpenTelemetryJavaagentTraceIdsTest {
    private val tracer = GlobalOpenTelemetry.getTracer("test")

    private fun <T> profiled(body: () -> T): T {
        Profiler.enter("OpenTelemetryJavaagentTraceIdsTest.profiled")
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
    fun `the tracer comes from the Java agent`() {
        val span = tracer.spanBuilder("probe").startSpan()
        try {
            assertTrue(span.spanContext.isValid, "The agent is not attached: the global tracer creates no real span")
        } finally {
            span.end()
        }
    }

    @Test
    fun `a span created in a profiled call is recorded without being made current`() {
        profiled {
            val span = tracer.spanBuilder("created").startSpan()
            try {
                assertEquals(span.spanContext.traceId, callInfo().traceId, "traceId")
                assertEquals(span.spanContext.spanId, callInfo().spanId, "spanId")
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
}
