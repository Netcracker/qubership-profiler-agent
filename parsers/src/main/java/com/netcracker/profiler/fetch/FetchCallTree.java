package com.netcracker.profiler.fetch;

import com.netcracker.profiler.analyzer.MergeTrees;
import com.netcracker.profiler.dom.ProfiledTreeStreamVisitor;
import com.netcracker.profiler.io.CallRowid;
import com.netcracker.profiler.sax.builders.ProfiledTreeBuilderMR;
import com.netcracker.profiler.sax.raw.MultiRepositoryVisitor;
import com.netcracker.profiler.sax.readers.ProfilerTraceReaderMR;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope("prototype")
public class FetchCallTree implements Runnable {
    private final ProfiledTreeStreamVisitor sv;
    private final CallRowid[] callIds;
    private final int paramsTrimSize;
    private final long begin;
    private final long end;

    @Autowired
    ApplicationContext context;

    private FetchCallTree() {
        throw new RuntimeException("no-args constructor not suppoorted");
    }

    public FetchCallTree(ProfiledTreeStreamVisitor sv, CallRowid[] callIds, int paramsTrimSize) {
        this(sv, callIds, paramsTrimSize, Long.MIN_VALUE, Long.MAX_VALUE);
    }

    public FetchCallTree(ProfiledTreeStreamVisitor sv, CallRowid[] callIds, int paramsTrimSize, long begin, long end) {
        this.sv = sv;
        this.callIds = callIds;
        this.paramsTrimSize = paramsTrimSize;
        this.begin = begin;
        this.end = end;
    }

    public void run() {
        ProfiledTreeStreamVisitor sv = new MergeTrees(this.sv);
        MultiRepositoryVisitor mrv = new ProfiledTreeBuilderMR(sv, paramsTrimSize, context);

        ProfilerTraceReaderMR reader = context.getBean(ProfilerTraceReaderMR.class, mrv);
        reader.read(callIds, begin, end);
    }
}
