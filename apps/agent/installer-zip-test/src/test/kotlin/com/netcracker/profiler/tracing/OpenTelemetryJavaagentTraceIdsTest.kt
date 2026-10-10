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
}
