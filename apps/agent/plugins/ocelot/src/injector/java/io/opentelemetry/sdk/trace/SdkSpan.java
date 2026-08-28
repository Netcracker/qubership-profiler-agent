package io.opentelemetry.sdk.trace;

import com.netcracker.profiler.agent.CallInfo;
import com.netcracker.profiler.agent.Profiler;

import io.opentelemetry.api.trace.SpanContext;

public class SdkSpan {

    public native SpanContext getSpanContext();

    public void logIdAfterStart$profiler() {
        if(Profiler.getState().sp == 0) return; //Do not log traceId/SpanId if it's created under not profiled code

        SpanContext context = getSpanContext();
        if (context == null) {
            return;
        }
        String traceId = context.getTraceId();
        if (traceId == null) {
            return;
        }
        // Populate end-to-end
        CallInfo callInfo = Profiler.getState().callInfo;
        String endToEndId = callInfo.getEndToEndId();
        if (endToEndId == null) {
            callInfo.setEndToEndId(traceId);
        }
        callInfo.setTraceId(traceId);
        if(callInfo.traceIdChanged()) {
            Profiler.event(traceId, "trace.id");
        }
    }

}
