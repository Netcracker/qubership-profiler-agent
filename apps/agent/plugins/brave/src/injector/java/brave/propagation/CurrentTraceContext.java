package brave.propagation;

import com.netcracker.profiler.agent.Profiler;
import com.netcracker.profiler.agent.TraceIds;

public class CurrentTraceContext {
    /**
     * The default current-context implementation before Brave 5.2.0. From 5.2.0 on it inherits
     * {@code newScope} from {@link ThreadLocalCurrentTraceContext}, and the hook lives there.
     */
    @SuppressWarnings("UnusedNestedClass")
    public static class Default {
        /** Records the span of the context that has just become current on this thread. */
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
}
