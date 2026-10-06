package com.netcracker.profiler.exception;

import java.io.IOException;

public class ProfilerAgentIOException extends IOException {
    public ProfilerAgentIOException() {
    }

    public ProfilerAgentIOException(String s) {
        super(s);
    }

    public ProfilerAgentIOException(String s, Throwable throwable) {
        super(s, throwable);
    }

    public ProfilerAgentIOException(Throwable throwable) {
        super(throwable);
    }
}
