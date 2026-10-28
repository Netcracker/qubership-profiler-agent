package io.opentelemetry.sdk.trace;

import com.netcracker.profiler.agent.TraceIds;

import io.opentelemetry.api.trace.SpanContext;

/** The SDK span up to 1.12, before it was renamed to {@code SdkSpan}. */
public class RecordEventsReadableSpan {

    public native SpanContext getSpanContext();

    public native SpanContext getParentSpanContext();

    /**
     * Records the span as soon as it exists, which covers a span the application never makes
     * current.
     *
     * <p>A parent that arrived from another process is recorded too. An entry point passes the
     * extracted context to the span builder and seldom makes it current, so this is the one place
     * that sees it.</p>
     */
    public void recordSpan$profiler() {
        SpanContext context = getSpanContext();
        if (context == null || !context.isValid()) {
            return;
        }
        SpanContext parent = getParentSpanContext();
        if (parent != null && parent.isValid() && parent.isRemote()) {
            TraceIds.recordRemoteParent(parent.getTraceId(), parent.getSpanId());
        }
        TraceIds.recordSpan(context.getTraceId(), context.getSpanId());
    }
}
