package com.netcracker.profiler.io.xlsx;

import com.netcracker.profiler.io.CallFilterer;

import com.google.inject.assistedinject.Assisted;

import java.io.OutputStream;

/**
 * Factory interface for creating CallsToXLSXListener instances.
 */
public interface CallsToXLSXListenerFactory {
    CallsToXLSXListener create(
        @Assisted("serverAddress") String serverAddress,
        @Assisted("cf") CallFilterer cf,
        @Assisted("out") OutputStream out
    );
}
