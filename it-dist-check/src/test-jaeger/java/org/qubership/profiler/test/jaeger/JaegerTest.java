package org.qubership.profiler.test.jaeger;

import org.qubership.profiler.agent.LocalState;
import org.qubership.profiler.agent.Profiler;
import org.qubership.profiler.test.ConfigurationTest;
import io.jaegertracing.Configuration;
import io.opentracing.Span;
import io.opentracing.Tracer;
import org.junit.Assert;
import org.testng.annotations.Test;

public class JaegerTest extends ConfigurationTest {
    @Test
    public void jaeger() {
        Profiler.enter("void org.qubership.profiler.test.JaegerTest.jaeger() (JaegerTest.java:24) [unknown jar]");
        try {
            Tracer tracer = new Configuration("test-tracer")
                .withTraceId128Bit(true)
                .getTracer();
            Span span = tracer.buildSpan("root operation")
                .withTag("hello", "world")
                .start();
            String traceId = span.context().toTraceId();
            Assert.assertNotNull("Jaeger.traceId is null: tracer does not work", traceId);
            span.finish();

            LocalState state = Profiler.getState();
            String endToEndId = state.callInfo.getEndToEndId();
            Assert.assertEquals(
                "state.callInfo.getEndToEndId() should be equal to Jaeger.traceId",
                endToEndId,
                traceId
            );
        } finally {
            Profiler.exit();
        }
    }
}
