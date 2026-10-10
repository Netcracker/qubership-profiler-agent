package io.opentelemetry.javaagent.shaded.io.opentelemetry.context;

import com.netcracker.profiler.agent.Profiler;
import com.netcracker.profiler.agent.TraceIds;

import io.opentelemetry.javaagent.shaded.io.opentelemetry.api.trace.Span;
import io.opentelemetry.javaagent.shaded.io.opentelemetry.api.trace.SpanContext;

/**
 * The copy of the default context storage that the OpenTelemetry Java agent keeps in the bootstrap
 * class loader, under a relocated package.
 *
 * <p>The agent's own instrumentations make their spans current through it, and the agent routes the
 * application's OpenTelemetry API to it as well, so this one hook covers both.</p>
 */
public class ThreadLocalContextStorage {
    /**
     * Records the span of the context that has just become current on this thread, as the hook on
     * the application's own {@code ThreadLocalContextStorage} does.
     */
    public void recordCurrentSpan$profiler(Context toAttach) {
        if (toAttach == null || Profiler.getState().sp == 0) {
            return;
        }
        Span span = Span.fromContextOrNull(toAttach);
        if (span == null) {
            return;
        }
        SpanContext context = span.getSpanContext();
        if (context == null || !context.isValid()) {
            return;
        }
        if (context.isRemote()) {
            TraceIds.recordRemoteParent(context.getTraceId(), context.getSpanId());
        } else {
            TraceIds.recordSpan(context.getTraceId(), context.getSpanId());
        }
    }
}
