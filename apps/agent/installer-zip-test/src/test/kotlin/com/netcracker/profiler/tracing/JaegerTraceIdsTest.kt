package com.netcracker.profiler.tracing

import com.netcracker.profiler.agent.CallInfo
import com.netcracker.profiler.agent.Profiler
import io.jaegertracing.internal.JaegerTracer
import io.jaegertracing.internal.reporters.InMemoryReporter
import io.jaegertracing.internal.samplers.ConstSampler
import io.opentracing.noop.NoopSpan
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/** Runs Jaeger under the agent and reads back the IDs the `jaeger` plugin stored for the profiled call. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class JaegerTraceIdsTest {
    private val tracer = JaegerTracer.Builder("test")
        .withReporter(InMemoryReporter())
        .withSampler(ConstSampler(true))
        .withTraceId128Bit()
        .build()

    @AfterAll
    fun close() = tracer.close()

    private fun <T> profiled(body: () -> T): T {
        Profiler.enter("JaegerTraceIdsTest.profiled")
        try {
            return body()
        } finally {
            Profiler.exit()
        }
    }

    private fun callInfo(): CallInfo = Profiler.getState().callInfo

    @Test
    fun `a span started in a profiled call is recorded without being activated`() {
        profiled {
            val span = tracer.buildSpan("started").start()
            try {
                assertEquals(span.context().toTraceId(), callInfo().traceId, "traceId")
                assertEquals(span.context().toSpanId(), callInfo().spanId, "spanId")
                assertEquals(span.context().toTraceId(), callInfo().endToEndId, "endToEndId")
            } finally {
                span.finish()
            }
        }
    }

    @Test
    fun `a span started elsewhere is recorded when it is activated`() {
        val span = tracer.buildSpan("handed over").start()
        try {
            profiled {
                assertNotEquals(span.context().toSpanId(), callInfo().spanId, "spanId before the span is active")
                tracer.activateSpan(span).use {
                    assertEquals(span.context().toTraceId(), callInfo().traceId, "traceId")
                    assertEquals(span.context().toSpanId(), callInfo().spanId, "spanId")
                }
            }
        } finally {
            span.finish()
        }
    }

    @Test
    fun `a no-op span activated before a real one leaves no empty ID`() {
        val span = tracer.buildSpan("after no-op").start()
        try {
            profiled {
                tracer.activateSpan(NoopSpan.INSTANCE).use {
                    assertNull(callInfo().traceId, "traceId under the no-op span")
                    assertNull(callInfo().spanId, "spanId under the no-op span")
                    assertNull(callInfo().endToEndId, "endToEndId under the no-op span")
                }
                tracer.activateSpan(span).use {
                    assertEquals(span.context().toTraceId(), callInfo().traceId, "traceId")
                    assertEquals(span.context().toSpanId(), callInfo().spanId, "spanId")
                    assertEquals(span.context().toTraceId(), callInfo().endToEndId, "endToEndId")
                }
            }
        } finally {
            span.finish()
        }
    }

    @Test
    fun `a no-op span activated after a real one keeps the recorded IDs`() {
        val span = tracer.buildSpan("before no-op").start()
        try {
            profiled {
                tracer.activateSpan(span).use {
                    tracer.activateSpan(NoopSpan.INSTANCE).use {
                        assertEquals(span.context().toTraceId(), callInfo().traceId, "traceId")
                        assertEquals(span.context().toSpanId(), callInfo().spanId, "spanId")
                    }
                }
            }
        } finally {
            span.finish()
        }
    }
}
