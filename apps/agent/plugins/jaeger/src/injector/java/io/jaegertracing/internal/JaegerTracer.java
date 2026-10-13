package io.jaegertracing.internal;

import com.netcracker.profiler.agent.Profiler;
import com.netcracker.profiler.agent.TraceIds;

import io.opentracing.Span;
import io.opentracing.SpanContext;

public class JaegerTracer {
    @SuppressWarnings("UnusedNestedClass")
    public class SpanBuilder {
        /**
         * Records the span as soon as it is started, which covers a span the application never
         * activates.
         *
         * <p>The builder declares {@code start()} twice, once per return type, and one calls the
         * other. Both match the rule, and the second report of the same span writes nothing.</p>
         */
        public void recordStartedSpan$profiler(Span span) {
            // start() itself is profiled, so a depth of 1 means no profiled call encloses it
            if (span == null || Profiler.getState().sp <= 1) {
                return;
            }
            SpanContext context = span.context();
            if (context == null) {
                return;
            }
            TraceIds.recordSpan(context.toTraceId(), context.toSpanId());
        }
    }
}
