package com.netcracker.profiler.sax.builders;

import com.netcracker.profiler.analyzer.DelayTreeTillReady;
import com.netcracker.profiler.analyzer.MergeTrees;
import com.netcracker.profiler.dom.ProfiledTreeStreamVisitor;
import com.netcracker.profiler.sax.raw.MultiRepositoryVisitor;
import com.netcracker.profiler.sax.raw.RepositoryVisitor;
import com.netcracker.profiler.util.ProfilerConstants;

public class ProfiledTreeBuilderMR extends MultiRepositoryVisitor {
    private final ProfiledTreeStreamVisitor sv;
    private final int paramsTrimSize;
    private final SuspendLogBuilderFactory suspendLogBuilderFactory;

    public ProfiledTreeBuilderMR(ProfiledTreeStreamVisitor sv, SuspendLogBuilderFactory suspendLogBuilderFactory) {
        this(ProfilerConstants.PROFILER_V1, sv, Integer.MAX_VALUE, suspendLogBuilderFactory);
    }

    public ProfiledTreeBuilderMR(ProfiledTreeStreamVisitor sv, int paramsTrimSize, SuspendLogBuilderFactory suspendLogBuilderFactory) {
        this(ProfilerConstants.PROFILER_V1, sv, paramsTrimSize, suspendLogBuilderFactory);
    }

    protected ProfiledTreeBuilderMR(int api, ProfiledTreeStreamVisitor sv, int paramsTrimSize, SuspendLogBuilderFactory suspendLogBuilderFactory) {
        super(api);
        this.sv = sv;
        this.paramsTrimSize = paramsTrimSize;
        this.suspendLogBuilderFactory = suspendLogBuilderFactory;
    }

    @Override
    public RepositoryVisitor visitRepository(String rootReference) {
        ProfiledTreeStreamVisitor delay = new DelayTreeTillReady(sv.asSkipVisitEnd());
        // When merging trees from different repositories, allow to sub-merge trees
        // Otherwise trees will get stuck until repository.visitDictionaryReady
        if (this.sv instanceof MergeTrees)
            delay = new MergeTrees(delay);
        return new ProfiledTreeBuilder(delay, paramsTrimSize, suspendLogBuilderFactory, rootReference);
    }

    @Override
    public void visitEnd() {
        sv.visitEnd();
    }
}
