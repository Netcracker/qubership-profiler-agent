package com.netcracker.profiler.agent;

public class ExceptionLogger {

    public static void dumpExceptionClass(Throwable t) {
        Profiler.event(t.getClass().getName(), "exception.class");
    }

    public static void dumpExceptionWithMessage(Throwable t) {
        Profiler.event(t.toString(), "exception.text");
    }

    public static void dumpExceptionWithMessageAndStacktrace(Throwable t) {
        Profiler.event(t, "exception");
    }

    public static void logExceptionClass(Throwable t) {
        Profiler.logError("exception.class: " + t.getClass().getName());
    }

    public static void logExceptionWithMessage(Throwable t) {
        Profiler.logError("exception.text: " + t.toString());
    }

    public static void logExceptionWithMessageAndStacktrace(Throwable t) {
        Profiler.logError("exception", t);
    }

    public static void callRed() {
        LocalState state = Profiler.getState();
        if(!state.callInfo.isCallRed) {
            state.callInfo.isCallRed = true;
            Profiler.event("1", "call.red");
        }
    }

    public static void callRed(Throwable t) {
        callRed();
    }
}
