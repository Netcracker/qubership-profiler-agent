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
