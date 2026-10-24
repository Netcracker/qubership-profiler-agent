package io.opentelemetry.javaagent.shaded.io.opentelemetry.context;

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
        if (toAttach == null) {
            // attach leaves the current context alone
            return;
        }
        SpanContext context = validSpanContext$profiler(toAttach);
        if (context == null) {
            TraceIds.recordCurrentSpan(null, null, false);
        } else {
            TraceIds.recordCurrentSpan(context.getTraceId(), context.getSpanId(), context.isRemote());
        }
    }

    /**
     * Reports the span of the context that closing a scope has just restored on this thread, as
     * the hook on the application's own {@code ThreadLocalContextStorage} does.
     */
    public void restoreCurrentSpan$profiler() {
        SpanContext context = validSpanContext$profiler(Context.current());
        if (context == null) {
            TraceIds.restoreCurrentSpan(null, null, false);
        } else {
            TraceIds.restoreCurrentSpan(context.getTraceId(), context.getSpanId(), context.isRemote());
        }
    }

    private static SpanContext validSpanContext$profiler(Context context) {
        Span span = Span.fromContextOrNull(context);
        if (span == null) {
            return null;
        }
        SpanContext spanContext = span.getSpanContext();
        return spanContext == null || !spanContext.isValid() ? null : spanContext;
    }

    /** The scope that {@code attach} returns from 1.22.0 on. Earlier releases return a lambda. */
    @SuppressWarnings("UnusedNestedClass")
    static class ScopeImpl {
        /**
         * Reports the span of the context that closing this scope has just restored. It repeats
         * the hook of the enclosing class, which is injected only where the lambda exists.
         */
        public void restoreCurrentSpan$profiler() {
            Span span = Span.fromContextOrNull(Context.current());
            SpanContext context = span == null ? null : span.getSpanContext();
            if (context == null || !context.isValid()) {
                TraceIds.restoreCurrentSpan(null, null, false);
            } else {
                TraceIds.restoreCurrentSpan(context.getTraceId(), context.getSpanId(), context.isRemote());
            }
        }
    }
}
