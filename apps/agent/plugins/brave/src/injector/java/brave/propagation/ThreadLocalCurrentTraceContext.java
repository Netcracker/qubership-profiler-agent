package brave.propagation;

import com.netcracker.profiler.agent.Profiler;
import com.netcracker.profiler.agent.TraceIds;

/** The default current-context implementation from Brave 5.2.0 on. */
public class ThreadLocalCurrentTraceContext {
    /**
     * Records the span of the context that has just become current on this thread, which covers a
     * thread that continues a trace without starting a span of its own.
     *
     * <p>Closing a scope restores the previous context without calling {@code newScope}, so the
     * span recorded for a call is the last one that became current, not the one that is current
     * now.</p>
     */
    public void recordCurrentSpan$profiler(TraceContext context) {
        // A null context clears the current span
        if (context == null || Profiler.getState().sp == 0) {
            return;
        }
        if (!Boolean.TRUE.equals(context.sampled())) {
            return;
        }
        TraceIds.recordSpan(context.traceIdString(), context.spanId());
    }
}
