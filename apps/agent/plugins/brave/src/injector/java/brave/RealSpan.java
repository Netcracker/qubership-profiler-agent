package brave;

import com.netcracker.profiler.agent.Profiler;
import com.netcracker.profiler.agent.TraceIds;

import brave.propagation.TraceContext;

public abstract class RealSpan extends Span {
    void logSpanIds$profiler() {
        if(Profiler.getState().sp <= 1) return; //Do not log traceId/SpanId if it's created under not profiled code
        TraceContext context = context();
        if (context == null) {
            return;
        }
        // The decision is null while it is deferred, and such a span is skipped like an unsampled one
        if (!Boolean.TRUE.equals(context.sampled())) {
            return;
        }
        String traceId = context.traceIdString();
        // The names every tracer plugin shares. The brave.* parameters below predate them and stay
        // for the searches that already use them.
        TraceIds.recordSpan(traceId, context.spanId());

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
