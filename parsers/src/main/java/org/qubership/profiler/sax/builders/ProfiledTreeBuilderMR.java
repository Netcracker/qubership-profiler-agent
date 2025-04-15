package org.qubership.profiler.sax.builders;

import org.qubership.profiler.analyzer.DelayTreeTillReady;
import org.qubership.profiler.analyzer.MergeTrees;
import org.qubership.profiler.dom.ProfiledTreeStreamVisitor;
import org.qubership.profiler.sax.raw.MultiRepositoryVisitor;
import org.qubership.profiler.sax.raw.RepositoryVisitor;
import org.qubership.profiler.util.ProfilerConstants;

import org.springframework.context.ApplicationContext;

public class ProfiledTreeBuilderMR extends MultiRepositoryVisitor {
    private final ProfiledTreeStreamVisitor sv;
    private final int paramsTrimSize;
    private final ApplicationContext context;

    public ProfiledTreeBuilderMR(ProfiledTreeStreamVisitor sv, ApplicationContext context) {
        this(ProfilerConstants.PROFILER_V1, sv, Integer.MAX_VALUE, context);
    }

    public ProfiledTreeBuilderMR(ProfiledTreeStreamVisitor sv, int paramsTrimSize, ApplicationContext context) {
        this(ProfilerConstants.PROFILER_V1, sv, paramsTrimSize, context);
    }

    protected ProfiledTreeBuilderMR(int api, ProfiledTreeStreamVisitor sv, int paramsTrimSize, ApplicationContext context) {
        super(api);
        this.sv = sv;
        this.paramsTrimSize = paramsTrimSize;
        this.context = context;
    }

    @Override
    public RepositoryVisitor visitRepository(String rootReference) {
        ProfiledTreeStreamVisitor delay = new DelayTreeTillReady(sv.asSkipVisitEnd());
        // When merging trees from different repositories, allow to sub-merge trees
        // Otherwise trees will get stuck until repository.visitDictionaryReady
        if (this.sv instanceof MergeTrees)
            delay = new MergeTrees(delay);
        return new ProfiledTreeBuilder(delay, paramsTrimSize, context, rootReference);
    }

    @Override
    public void visitEnd() {
        sv.visitEnd();
    }
}
