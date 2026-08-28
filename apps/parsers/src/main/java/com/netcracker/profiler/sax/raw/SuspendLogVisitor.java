package com.netcracker.profiler.sax.raw;

import com.netcracker.profiler.util.ProfilerConstants;

public class SuspendLogVisitor implements ISuspendLogVisitor {
    /**
     * The API version implemented by this visitor. The value of this field
     * must be one of {@link ProfilerConstants#PROFILER_V1}.
     */
    protected final int api;
    protected final SuspendLogVisitor sv;

    public SuspendLogVisitor(int api) {
        this(api, null);
    }

    public SuspendLogVisitor(int api, SuspendLogVisitor sv) {
        this.api = api;
        this.sv = sv;
    }

    public void visitHiccup(long date, int delay) {
        if (sv != null)
            sv.visitHiccup(date, delay);
    }

    public void visitEnd() {
        if (sv != null)
            sv.visitEnd();
    }

    public SuspendLogVisitor asSkipVisitEnd() {
        return new SuspendLogVisitor(api, this) {
            @Override
            public void visitEnd() {
                // No operation
            }

            @Override
            public SuspendLogVisitor asSkipVisitEnd() {
                return this;
            }
        };
    }
}
