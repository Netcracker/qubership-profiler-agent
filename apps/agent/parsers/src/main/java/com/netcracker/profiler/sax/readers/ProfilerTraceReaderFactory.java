package com.netcracker.profiler.sax.readers;

import com.netcracker.profiler.sax.raw.RepositoryVisitor;

import com.google.inject.assistedinject.Assisted;

public interface ProfilerTraceReaderFactory {
    ProfilerTraceReader newTraceReader(@Assisted("rv") RepositoryVisitor rv, @Assisted("rootReference") String rootReference);
}
