package com.netcracker.profiler.io.xlsx;

import com.netcracker.profiler.io.CallFilterer;

import com.google.inject.assistedinject.Assisted;

import java.io.OutputStream;
import java.util.Map;

/**
 * Factory interface for creating AggregateCallsToXLSXListener instances.
 */
public interface AggregateCallsToXLSXListenerFactory {
    AggregateCallsToXLSXListener create(
        @Assisted("cf") CallFilterer cf,
        @Assisted("out") OutputStream out,
        @Assisted("formatContext") Map<String, Object> formatContext
    );
}
