package com.netcracker.profiler.sax.builders;

import com.google.inject.assistedinject.Assisted;

/**
 * Factory interface for creating SuspendLogBuilder instances.
 */
public interface SuspendLogBuilderFactory {
    SuspendLogBuilder create(@Assisted("rootReference") String rootReference);
}
