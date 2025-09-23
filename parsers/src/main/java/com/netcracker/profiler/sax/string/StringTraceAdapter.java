package com.netcracker.profiler.sax.string;

import com.netcracker.profiler.sax.raw.TraceVisitor;
import com.netcracker.profiler.sax.raw.TreeRowid;
import com.netcracker.profiler.sax.raw.TreeTraceVisitor;
import com.netcracker.profiler.util.ProfilerConstants;

public class StringTraceAdapter extends TraceVisitor {
    private final StringRepositoryAdapter ra;

    public StringTraceAdapter(StringRepositoryAdapter ra, TraceVisitor tv) {
        this(ProfilerConstants.PROFILER_V1, ra, tv);
    }

    protected StringTraceAdapter(int api, StringRepositoryAdapter ra, TraceVisitor tv) {
        super(api, tv);
        this.ra = ra;
    }

    @Override
    public StringTreeTraceAdapter visitTree(TreeRowid rowid) {
        TreeTraceVisitor ttv = super.visitTree(rowid);
        if (ttv == null)
            return null;
        return new StringTreeTraceAdapter(ra, ttv);
    }

    public StringTraceAdapter asSkipVisitEnd() {
        return new StringTraceAdapter(api, ra, this) {
            @Override
            public void visitEnd() {
                // No operation
            }

            @Override
            public StringTraceAdapter asSkipVisitEnd() {
                return this;
            }
        };
    }
}
