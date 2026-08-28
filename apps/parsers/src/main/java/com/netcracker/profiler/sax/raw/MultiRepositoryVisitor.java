package com.netcracker.profiler.sax.raw;

import com.netcracker.profiler.util.ProfilerConstants;

public class MultiRepositoryVisitor {
    /**
     * The API version implemented by this visitor. The value of this field
     * must be one of {@link ProfilerConstants#PROFILER_V1}.
     */
    protected final int api;

    protected final MultiRepositoryVisitor mrv;


    public MultiRepositoryVisitor(int api) {
        this(api, null);
    }

    public MultiRepositoryVisitor(int api, MultiRepositoryVisitor mrv) {
        this.api = api;
        this.mrv = mrv;
    }

    public RepositoryVisitor visitRepository(String rootReference) {
        if (mrv != null)
            return mrv.visitRepository(rootReference);
        return null;
    }

    public void visitEnd() {
        if (mrv != null)
            mrv.visitEnd();
    }

    public MultiRepositoryVisitor asSkipVisitEnd() {
        return new MultiRepositoryVisitor(api, this) {
            @Override
            public void visitEnd() {
                // No operation
            }

            @Override
            public MultiRepositoryVisitor asSkipVisitEnd() {
                return this;
            }
        };
    }
}
