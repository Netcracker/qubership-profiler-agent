package com.netcracker.profiler.cloud.transport;

public class ProfilerProtocolTimeoutException extends ProfilerProtocolException{
    public ProfilerProtocolTimeoutException(String message) {
        super(message);
    }

    public ProfilerProtocolTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }

    public ProfilerProtocolTimeoutException(Throwable cause) {
        super(cause);
    }
}
