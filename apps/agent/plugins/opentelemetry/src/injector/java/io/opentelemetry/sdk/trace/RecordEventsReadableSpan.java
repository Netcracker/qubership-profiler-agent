package io.opentelemetry.sdk.trace;

import com.netcracker.profiler.agent.TraceIds;

import io.opentelemetry.api.trace.SpanContext;

/** The SDK span up to 1.12, before it was renamed to {@code SdkSpan}. */
public class RecordEventsReadableSpan {

    public native SpanContext getSpanContext();

    /**
     * Records the span as soon as it exists, which covers a span the application never makes
     * current.
     */
    public void recordSpan$profiler() {
        SpanContext context = getSpanContext();
        if (context == null || !context.isValid()) {
            return;
        }
        TraceIds.recordSpan(context.getTraceId(), context.getSpanId());
    }
}
