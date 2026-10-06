package com.netcracker.profiler.sax.stack;

import com.netcracker.profiler.util.ProfilerConstants;

public class DumpsVisitor {
    /**
     * The API version implemented by this visitor. The value of this field
     * must be one of {@link ProfilerConstants#PROFILER_V1}.
     */
    protected final int api;

    protected final DumpsVisitor dv;

    public DumpsVisitor(int api) {
        this(api, null);
    }

    public DumpsVisitor(int api, DumpsVisitor dv) {
        this.api = api;
        this.dv = dv;
    }

    public DumpVisitor visitDump() {
        if (dv != null)
            return dv.visitDump();
        return null;
    }

    public void visitEnd() {
        if (dv != null)
            dv.visitEnd();
    }

    public DumpsVisitor asSkipVisitEnd() {
        return new DumpsVisitor(api, this) {
            @Override
            public void visitEnd() {
                // No operation
            }

            @Override
            public DumpsVisitor asSkipVisitEnd() {
                return this;
            }
        };
    }
}
