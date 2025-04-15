package org.qubership.profiler.sax.string;

import org.qubership.profiler.sax.raw.TreeTraceVisitor;
import org.qubership.profiler.sax.values.ValueHolder;
import org.qubership.profiler.util.ProfilerConstants;

public class StringTreeTraceAdapter extends TreeTraceVisitor {
    protected final StringRepositoryAdapter ra;

    public StringTreeTraceAdapter(StringRepositoryAdapter ra, TreeTraceVisitor tv) {
        this(ProfilerConstants.PROFILER_V1, ra, tv);
    }

    protected StringTreeTraceAdapter(int api, StringRepositoryAdapter ra, TreeTraceVisitor tv) {
        super(api, tv);
        this.ra = ra;
    }

    public void visitEnter(String methodId) {
        super.visitEnter(ra.allocateId(methodId));
    }

    public void visitLabel(String labelId, ValueHolder value) {
        super.visitLabel(ra.allocateId(labelId), value);
    }

    @Override
    public StringTreeTraceAdapter asSkipVisitEnd() {
        return new StringTreeTraceAdapter(api, ra, this) {
            @Override
            public void visitEnd() {
                // No operation
            }

            @Override
            public StringTreeTraceAdapter asSkipVisitEnd() {
                return this;
            }
        };
    }
}
