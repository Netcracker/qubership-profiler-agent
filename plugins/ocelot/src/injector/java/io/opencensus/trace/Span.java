package io.opencensus.trace;

import org.qubership.profiler.agent.CallInfo;
import org.qubership.profiler.agent.Profiler;

public class Span {

    public native SpanContext getContext();

    public void logIdAfterStart$profiler() {
        if(Profiler.getState().sp == 0) return; //Do not log traceId/SpanId if it's created under not profiled code
        SpanContext context = getContext();
        if (context == null) {
            return;
        }
        TraceId traceIdObj = context.getTraceId();
        if(traceIdObj == null) {
            return;
        }
        String traceId = traceIdObj.toLowerBase16();
        // Populate end-to-end
        CallInfo callInfo = Profiler.getState().callInfo;
        String endToEndId = callInfo.getEndToEndId();
        if (endToEndId == null) {
            callInfo.setEndToEndId(traceId);
        }
        callInfo.setTraceId(traceId);
        Profiler.event(traceId, "trace.id");

        SpanId spanIdObj = context.getSpanId();
        if(spanIdObj == null) {
            return;
        }
        String spanId= spanIdObj.toLowerBase16();
        Profiler.event(spanId, "span.id");
    }

}
