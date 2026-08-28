package com.netcracker.profiler.io;

import com.google.inject.assistedinject.Assisted;

import java.io.PrintWriter;

/**
 * Factory interface for creating CallToJS instances with runtime parameters.
 */
public interface CallToJSFactory {
    CallToJS create(
            @Assisted("out") PrintWriter out,
            @Assisted("cf") CallFilterer cf
    );
}
