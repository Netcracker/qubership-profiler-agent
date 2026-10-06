package io.undertow.server;

import com.netcracker.profiler.agent.Profiler;
import com.netcracker.profiler.agent.TimerCache;

public class Connectors {
    private static volatile boolean skipRequestTime$profiler;

    public static void dumpQueueWaitTime$profiler(HttpServerExchange exchange) {
        if (exchange != null && !skipRequestTime$profiler) {
            // Obtain current time as early as possible
            long msCurrentTime = TimerCache.now;

            long nsRequestStartTime;
            try {
                nsRequestStartTime = exchange.getRequestStartTime();
            } catch (Throwable t) {
                // In case we work on a very old Undertow version without 'HttpServerExchange.getRequestStartTime' method
                skipRequestTime$profiler = true;
                return;
            }

            // recording of request start time could be disabled on server level
            if (nsRequestStartTime != 0 && nsRequestStartTime != -1) {
                // Request time is recorded in nanoseconds, which have meaning only in relative context,
                // so we need to compare them with some reference point in the past
                long nsStartTime = TimerCache.startTimeNano;
                long msStartTime = TimerCache.startTime;
                long delta = (nsRequestStartTime - nsStartTime) / 1000000;
                if (delta > 0) {
                    long msRequestStartTime = msStartTime + delta;
                    long queueWaitTime = msCurrentTime - msRequestStartTime;
                    if (queueWaitTime > 0) {
                        Profiler.getState().callInfo.addQueueWait(queueWaitTime);
                    }
                }
            }
        }
    }
}
