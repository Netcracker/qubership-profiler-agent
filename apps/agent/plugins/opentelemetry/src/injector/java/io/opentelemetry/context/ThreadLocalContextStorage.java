package io.opentelemetry.context;

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
     * <p>The hook reports outside a profiled call too. {@code Context.wrap} makes the context
     * current before the task runs, so the call that the task starts finds the span through what
     * {@code TraceIds} remembers for the thread.</p>
     *
     * <p>Inside a call, the span recorded is the last one that became current, not the one that is
     * current now: a span that a closed scope restores is remembered but not recorded again.</p>
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
     * Reports the span of the context that closing a scope has just restored on this thread.
     *
     * <p>Without it, a span that became current outside a profiled call would stay remembered
     * after its scope, and the next call on the thread would record it. This is the hook for the
     * lambda that {@code attach} returns up to 1.21.</p>
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
