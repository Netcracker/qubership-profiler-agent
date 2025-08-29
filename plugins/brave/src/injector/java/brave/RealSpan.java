package brave;

import org.qubership.profiler.agent.CallInfo;
import org.qubership.profiler.agent.Profiler;

import brave.propagation.TraceContext;

public abstract class RealSpan extends Span {
    void logSpanIds$profiler() {
        if(Profiler.getState().sp <= 1) return; //Do not log traceId/SpanId if it's created under not profiled code
        TraceContext context = context();
        if (context == null) {
            return;
        }
        if (!context.sampled()) {
            return;
        }
        String traceId = context.traceIdString();

        // Populate end-to-end
        CallInfo callInfo = Profiler.getState().callInfo;
        String endToEndId = callInfo.getEndToEndId();
        if (endToEndId == null) {
            callInfo.setEndToEndId(traceId);
        }
        callInfo.setTraceId(traceId);

        Profiler.event(traceId, "brave.trace_id");

        Long parentId = context.parentId();
        if (parentId != null) {
            Profiler.event(Long.toHexString(parentId), "brave.parent_id");
        }
        Profiler.event(Long.toHexString(context.spanId()), "brave.span_id");
    }

    void logTag$profiler(String name, String value) {
        if(Profiler.getState().sp <= 1) return; //Do not log traceId/SpanId if it's created under not profiled code
        Profiler.event(value, name);
    }
}
