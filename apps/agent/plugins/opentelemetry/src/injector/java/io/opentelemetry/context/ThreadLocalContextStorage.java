package io.opentelemetry.context;

import com.netcracker.profiler.agent.Profiler;
import com.netcracker.profiler.agent.TraceIds;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;

public class ThreadLocalContextStorage {
    /**
     * Records the span of the context that has just become current on this thread.
     *
     * <p>This is the path that reaches a thread that creates no span of its own: a task wrapped with
     * {@code Context.wrap}, or a handler that makes an extracted context current. A span that
     * arrived from another process is recorded as the parent, because it does not run here.</p>
     *
     * <p>Closing a scope restores the previous context without calling {@code attach}, so the span
     * recorded for a call is the last one that became current, not the one that is current now.</p>
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
