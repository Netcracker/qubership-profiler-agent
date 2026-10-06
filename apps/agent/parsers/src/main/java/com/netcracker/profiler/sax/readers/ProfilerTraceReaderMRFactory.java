package com.netcracker.profiler.sax.readers;

import com.netcracker.profiler.sax.raw.MultiRepositoryVisitor;

import com.google.inject.assistedinject.Assisted;

/**
 * Factory interface for creating ProfilerTraceReaderMR instances.
 */
public interface ProfilerTraceReaderMRFactory {
    ProfilerTraceReaderMR create(@Assisted("mrv") MultiRepositoryVisitor mrv);
}
