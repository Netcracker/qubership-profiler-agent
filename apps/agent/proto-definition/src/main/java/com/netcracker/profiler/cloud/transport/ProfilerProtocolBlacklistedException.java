package com.netcracker.profiler.cloud.transport;

public class ProfilerProtocolBlacklistedException extends ProfilerProtocolException {

    public ProfilerProtocolBlacklistedException(String message) {
        super(message);
    }

    public ProfilerProtocolBlacklistedException(String message, Throwable cause) {
        super(message, cause);
    }

    public ProfilerProtocolBlacklistedException(Throwable cause) {
        super(cause);
    }
}
