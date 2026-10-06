package com.netcracker.profiler.sax.raw;

import com.netcracker.profiler.sax.values.ClobValue;
import com.netcracker.profiler.util.ProfilerConstants;

public class ClobValueVisitor {
    /**
     * The API version implemented by this visitor. The value of this field
     * must be one of {@link ProfilerConstants#PROFILER_V1}.
     */
    protected final int api;

    protected final ClobValueVisitor cv;

    public ClobValueVisitor(int api) {
        this(api, null);
    }

    public ClobValueVisitor(int api, ClobValueVisitor cv) {
        this.api = api;
        this.cv = cv;
    }

    public void acceptValue(ClobValue clob, StrReader chars) {
        if (cv != null)
            cv.acceptValue(clob, chars);
    }

    public void visitEnd() {
        if (cv != null)
            cv.visitEnd();
    }

    public ClobValueVisitor asSkipVisitEnd() {
        return new ClobValueVisitor(api, this) {
            @Override
            public void visitEnd() {
                // No operation
            }

            @Override
            public ClobValueVisitor asSkipVisitEnd() {
                return this;
            }
        };
    }
}
