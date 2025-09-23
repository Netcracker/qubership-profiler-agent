package com.netcracker.profiler.sax.raw;

import com.netcracker.profiler.util.ProfilerConstants;

/**
 * A visitor to split profiling event tree by tree
 */
public class TraceVisitor {
    /**
     * The API version implemented by this visitor. The value of this field
     * must be one of {@link ProfilerConstants#PROFILER_V1}.
     */
    protected final int api;

    protected final TraceVisitor tv;

    public TraceVisitor(int api) {
        this(api, null);
    }

    public TraceVisitor(int api, TraceVisitor tv) {
        this.api = api;
        this.tv = tv;
    }

    public TreeTraceVisitor visitTree(TreeRowid rowid) {
        if (tv != null)
            return tv.visitTree(rowid);
        return null;
    }

    public void visitEnd() {
        if (tv != null)
            tv.visitEnd();
    }

    public TraceVisitor asSkipVisitEnd() {
        return new TraceVisitor(api, this) {
            @Override
            public void visitEnd() {
                // No operation
            }

            @Override
            public TraceVisitor asSkipVisitEnd() {
                return this;
            }
        };
    }
}
