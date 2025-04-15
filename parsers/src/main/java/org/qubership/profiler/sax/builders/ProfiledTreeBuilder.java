package org.qubership.profiler.sax.builders;

import org.qubership.profiler.dom.ProfiledTreeStreamVisitor;
import org.qubership.profiler.io.SuspendLog;
import org.qubership.profiler.sax.raw.*;
import org.qubership.profiler.util.ProfilerConstants;

import org.springframework.context.ApplicationContext;

public class ProfiledTreeBuilder extends RepositoryVisitor {
    private final ProfiledTreeStreamVisitor sv;
    private final DictionaryBuilder db;
    private final SuspendLogBuilder sb;
    private final ClobValuesBuilder cb;
    private final ApplicationContext context;

    public ProfiledTreeBuilder(ProfiledTreeStreamVisitor sv, int paramsTrimSize, ApplicationContext context, String rootReference) {
        this(ProfilerConstants.PROFILER_V1, sv, paramsTrimSize, context, rootReference);
    }

    protected ProfiledTreeBuilder(int api, ProfiledTreeStreamVisitor sv, int paramsTrimSize, ApplicationContext context, String rootReference) {
        super(api);
        this.sv = sv;
        this.context = context;
        db = new DictionaryBuilder();
        sb = context.getBean(SuspendLogBuilder.class, rootReference);
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
