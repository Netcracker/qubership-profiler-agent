package com.netcracker.profiler.timeout;

public class ProfilerTimeoutException extends RuntimeException {

    public ProfilerTimeoutException(int timeout) {
        super("Configured timeout "+timeout+"ms was exceeded. Please narrow your selection or increase timeout.");
    }

}
