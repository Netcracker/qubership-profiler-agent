package com.netcracker.profiler.sax.raw;

import com.netcracker.profiler.util.ProfilerConstants;

public class RepositoryVisitor {
    /**
     * The API version implemented by this visitor. The value of this field
     * must be one of {@link ProfilerConstants#PROFILER_V1}.
     */
    protected final int api;

    protected final RepositoryVisitor rv;

    public RepositoryVisitor(int api) {
        this(api, null);
    }

    public RepositoryVisitor(int api, RepositoryVisitor rv) {
        this.api = api;
        this.rv = rv;
    }

    public TraceVisitor visitTrace() {
        if (rv != null)
            return rv.visitTrace();
        return null;
    }

    public SuspendLogVisitor visitSuspendLog() {
        if (rv != null)
            return rv.visitSuspendLog();
        return null;
    }

    public DictionaryVisitor visitDictionary() {
        if (rv != null)
            return rv.visitDictionary();
        return null;
    }

    public ClobValueVisitor visitClobValues() {
        if (rv != null)
            return rv.visitClobValues();
        return null;
    }

    public void visitEnd() {
        if (rv != null)
            rv.visitEnd();
    }

    public RepositoryVisitor asSkipVisitEnd() {
        return new RepositoryVisitor(api, this) {
            @Override
            public void visitEnd() {
                // No operation
            }

            @Override
            public RepositoryVisitor asSkipVisitEnd() {
                return this;
            }
        };
    }
}
