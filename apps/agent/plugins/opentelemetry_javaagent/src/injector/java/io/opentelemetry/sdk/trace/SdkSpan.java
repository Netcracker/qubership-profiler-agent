package io.opentelemetry.sdk.trace;

import com.netcracker.profiler.agent.TraceIds;

import io.opentelemetry.javaagent.shaded.io.opentelemetry.api.trace.SpanContext;

/**
 * The SDK span inside the OpenTelemetry Java agent. It keeps the name it has in the SDK, and its
 * methods return the relocated API types.
 */
public class SdkSpan {

    public native SpanContext getSpanContext();

    public native SpanContext getParentSpanContext();

    /**
     * Records the span as soon as it exists, and its parent when the parent arrived from another
     * process.
     */
    public void recordAgentSpan$profiler() {
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
