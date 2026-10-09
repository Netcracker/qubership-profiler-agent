package io.opentracing.util;

import com.netcracker.profiler.agent.Profiler;
import com.netcracker.profiler.agent.TraceIds;

import io.opentracing.Span;
import io.opentracing.SpanContext;

/** The scope manager a Jaeger tracer uses unless the application supplies another. */
public class ThreadLocalScopeManager {
    /**
     * Records the span that has just become active on this thread, which covers a thread that
     * continues a trace without starting a span of its own.
     *
     * <p>The class belongs to OpenTracing, not to Jaeger, so the hook also fires for any other
     * tracer that keeps the default scope manager.</p>
     *
     * <p>Closing a scope restores the previous span without calling {@code activate}, so the span
     * recorded for a call is the last one that became active, not the one that is active now.</p>
     */
    public void recordActiveSpan$profiler(Span span) {
        if (span == null || Profiler.getState().sp == 0) {
            return;
        }
        SpanContext context = span.context();
        if (context == null) {
            return;
        }
        TraceIds.recordSpan(context.toTraceId(), context.toSpanId());
    }
}
