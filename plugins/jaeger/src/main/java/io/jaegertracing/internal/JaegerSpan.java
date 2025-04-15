package io.jaegertracing.internal;

import org.qubership.profiler.agent.CallInfo;
import org.qubership.profiler.agent.Profiler;

import io.opentracing.Span;
import io.opentracing.SpanContext;

public abstract class JaegerSpan implements Span {
    static void logIdAfterStart$profiler(Span span) {
        if(Profiler.getState().sp <= 1) return; //Do not log traceId/SpanId if it's created under not profiled code
        SpanContext context = span.context();
        if (context == null) {
            return;
        }
        String traceId = context.toTraceId();
        // Populate end-to-end
        CallInfo callInfo = Profiler.getState().callInfo;
        String endToEndId = callInfo.getEndToEndId();
        if (endToEndId == null) {
            callInfo.setEndToEndId(traceId);
        }
        callInfo.setTraceId(traceId);

        Profiler.event(traceId, "trace.id");
        Profiler.event(context.toSpanId(), "span.id");
    }
}
