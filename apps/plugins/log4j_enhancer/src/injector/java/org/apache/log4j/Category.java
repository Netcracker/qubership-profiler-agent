package org.apache.log4j;

import com.netcracker.profiler.agent.Profiler;

public class Category {
    protected static void logWritten$profiler(Object message) {
        Profiler.getState().callInfo.logWritten += getMessageLength$profiler(message);
    }

    protected static void logGenerated$profiler(Object message) {
        Profiler.getState().callInfo.logGenerated += getMessageLength$profiler(message);
    }

    private static int getMessageLength$profiler(Object message) {
        if (message instanceof String)
            return ((String) message).length();
        if (message instanceof StringBuffer)
            return ((StringBuffer) message).length();
        return 0;
    }
}
