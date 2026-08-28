package com.netcracker.profiler.fetch;

import com.netcracker.profiler.analyzer.MergeTrees;
import com.netcracker.profiler.dom.ProfiledTreeStreamVisitor;
import com.netcracker.profiler.io.CallRowid;
import com.netcracker.profiler.sax.builders.ProfiledTreeBuilderMR;
import com.netcracker.profiler.sax.builders.SuspendLogBuilderFactory;
import com.netcracker.profiler.sax.raw.MultiRepositoryVisitor;
import com.netcracker.profiler.sax.readers.ProfilerTraceReaderMR;
import com.netcracker.profiler.sax.readers.ProfilerTraceReaderMRFactory;

import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;

/**
 * Prototype-scoped class - create instances via {@code FetchCallTreeFactory}.
 */
public class FetchCallTree implements Runnable {
    private final ProfiledTreeStreamVisitor sv;
    private final CallRowid[] callIds;
    private final int paramsTrimSize;
    private final long begin;
    private final long end;
    private final SuspendLogBuilderFactory suspendLogBuilderFactory;
    private final ProfilerTraceReaderMRFactory profilerTraceReaderMRFactory;

    @AssistedInject
    public FetchCallTree(
            @Assisted("sv") ProfiledTreeStreamVisitor sv,
            @Assisted("callIds") CallRowid[] callIds,
            @Assisted("paramsTrimSize") int paramsTrimSize,
            SuspendLogBuilderFactory suspendLogBuilderFactory,
            ProfilerTraceReaderMRFactory profilerTraceReaderMRFactory) {
        this(sv, callIds, paramsTrimSize, Long.MIN_VALUE, Long.MAX_VALUE, suspendLogBuilderFactory, profilerTraceReaderMRFactory);
    }

    @AssistedInject
    public FetchCallTree(
            @Assisted("sv") ProfiledTreeStreamVisitor sv,
            @Assisted("callIds") CallRowid[] callIds,
            @Assisted("paramsTrimSize") int paramsTrimSize,
            @Assisted("begin") long begin,
            @Assisted("end") long end,
            SuspendLogBuilderFactory suspendLogBuilderFactory,
            ProfilerTraceReaderMRFactory profilerTraceReaderMRFactory) {
        this.sv = sv;
        this.callIds = callIds;
        this.paramsTrimSize = paramsTrimSize;
        this.begin = begin;
        this.end = end;
        this.suspendLogBuilderFactory = suspendLogBuilderFactory;
        this.profilerTraceReaderMRFactory = profilerTraceReaderMRFactory;
    }

    public void run() {
        ProfiledTreeStreamVisitor sv = new MergeTrees(this.sv);
        MultiRepositoryVisitor mrv = new ProfiledTreeBuilderMR(sv, paramsTrimSize, suspendLogBuilderFactory);

        ProfilerTraceReaderMR reader = profilerTraceReaderMRFactory.create(mrv);
        reader.read(callIds, begin, end);
    }
}
