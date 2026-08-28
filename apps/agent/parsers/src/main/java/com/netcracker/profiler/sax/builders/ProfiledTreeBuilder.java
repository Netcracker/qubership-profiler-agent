package com.netcracker.profiler.sax.builders;

import com.netcracker.profiler.dom.ProfiledTreeStreamVisitor;
import com.netcracker.profiler.io.SuspendLog;
import com.netcracker.profiler.sax.raw.*;
import com.netcracker.profiler.util.ProfilerConstants;

public class ProfiledTreeBuilder extends RepositoryVisitor {
    private final ProfiledTreeStreamVisitor sv;
    private final DictionaryBuilder db;
    private final SuspendLogBuilder sb;
    private final ClobValuesBuilder cb;

    public ProfiledTreeBuilder(ProfiledTreeStreamVisitor sv, int paramsTrimSize, SuspendLogBuilderFactory suspendLogBuilderFactory, String rootReference) {
        this(ProfilerConstants.PROFILER_V1, sv, paramsTrimSize, suspendLogBuilderFactory, rootReference);
    }

    protected ProfiledTreeBuilder(int api, ProfiledTreeStreamVisitor sv, int paramsTrimSize, SuspendLogBuilderFactory suspendLogBuilderFactory, String rootReference) {
        super(api);
        this.sv = sv;
        db = new DictionaryBuilder();
        sb = suspendLogBuilderFactory.create(rootReference);
        cb = new ClobValuesBuilder(paramsTrimSize);

    }

    @Override
    public TraceVisitor visitTrace() {
        return new MakeTreesFromTrace(sv.asSkipVisitEnd(), db.get(), getSuspendLog(), cb.get());
    }

    @Override
    public SuspendLogVisitor visitSuspendLog() {
        return sb;
    }

    @Override
    public ClobValueVisitor visitClobValues() {
        return cb;
    }

    @Override
    public DictionaryVisitor visitDictionary() {
        return db;
    }

    @Override
    public void visitEnd() {
        super.visitEnd();
        sv.visitDictionaryReady();
        sv.visitEnd();
    }

    private SuspendLog getSuspendLog() {
        return sb.get();
    }
}
