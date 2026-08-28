package com.netcracker.profiler.sax.stack;

import com.netcracker.profiler.threaddump.parser.ThreadInfo;
import com.netcracker.profiler.util.ProfilerConstants;


public class DumpVisitor {
    /**
     * The API version implemented by this visitor. The value of this field
     * must be one of {@link ProfilerConstants#PROFILER_V1}.
     */
    protected final int api;

    protected final DumpVisitor dv;

    public DumpVisitor(int api) {
        this(api, null);
    }

    public DumpVisitor(int api, DumpVisitor dv) {
        this.api = api;
        this.dv = dv;
    }

    public void visitThread(ThreadInfo thread) {
        if (dv != null)
            dv.visitThread(thread);
    }

    public void visitEnd() {
        if (dv != null)
            dv.visitEnd();
    }

    public DumpVisitor asSkipVisitEnd() {
        return new DumpVisitor(api, this) {
            @Override
            public void visitEnd() {
                // No operation
            }

            @Override
            public DumpVisitor asSkipVisitEnd() {
                return this;
            }
        };
    }
}
