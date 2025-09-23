package com.netcracker.profiler.util;

public interface ProfilerConstants {
    // Version of the SAX API
    // In new constant will be added in case of API upgrade, while keeping backward compatibility for old usages
    public static final int PROFILER_V1 = 1;

    String REACTOR_CALLS_STREAM = "reactor_calls";
    int CALL_HEADER_MAGIC = 0xfffefdfc;
}
