package com.netcracker.profiler.tracing

import brave.Tracing
import com.netcracker.profiler.agent.CallInfo
import com.netcracker.profiler.agent.Profiler
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/** Runs Brave under the agent and reads back the IDs the `brave` plugin stored for the profiled call. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class BraveTraceIdsTest {
    private val tracing = Tracing.newBuilder().traceId128Bit(true).build()
    private val tracer = tracing.tracer()

    @AfterAll
    fun close() = tracing.close()

    private fun <T> profiled(body: () -> T): T {
        Profiler.enter("BraveTraceIdsTest.profiled")
        try {
            return body()
        } finally {
            Profiler.exit()
        }
    }

    private fun callInfo(): CallInfo = Profiler.getState().callInfo

    @Test
    fun `a span started in a profiled call is recorded without being made current`() {
        profiled {
            val span = tracer.newTrace().name("started").start()
            try {
                assertEquals(span.context().traceIdString(), callInfo().traceId, "traceId")
                assertEquals(span.context().spanIdString(), callInfo().spanId, "spanId")
                assertEquals(span.context().traceIdString(), callInfo().endToEndId, "endToEndId")
            } finally {
                span.finish()
            }
        }
    }

    @Test
    fun `a span started elsewhere is recorded when its context becomes current`() {
        val span = tracer.newTrace().name("handed over").start()
        try {
            profiled {
                assertNotEquals(span.context().spanIdString(), callInfo().spanId, "spanId before the span is current")
                tracing.currentTraceContext().newScope(span.context()).use {
                    assertEquals(span.context().traceIdString(), callInfo().traceId, "traceId")
                    assertEquals(span.context().spanIdString(), callInfo().spanId, "spanId")
                }
            }
        } finally {
            span.finish()
        }
    }
}
