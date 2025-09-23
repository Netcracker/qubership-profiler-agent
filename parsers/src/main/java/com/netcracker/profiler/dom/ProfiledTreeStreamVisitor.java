package com.netcracker.profiler.dom;

import com.netcracker.profiler.util.ProfilerConstants;

public class ProfiledTreeStreamVisitor {
    /**
     * The API version implemented by this visitor. The value of this field
     * must be one of {@link ProfilerConstants#PROFILER_V1}.
     */
    protected final int api;
    protected ProfiledTreeStreamVisitor sv;

    public ProfiledTreeStreamVisitor(int api) {
        this(api, null);
    }

    public ProfiledTreeStreamVisitor(int api, ProfiledTreeStreamVisitor sv) {
        this.api = api;
        this.sv = sv;
    }

    public void visitTree(ProfiledTree tree) {
        if (sv != null)
            sv.visitTree(tree);
    }

    public void visitDictionaryReady() {
        if (sv != null)
            sv.visitDictionaryReady();
    }

    public void visitEnd() {
        if (sv != null)
            sv.visitEnd();
    }

    public void onError(Throwable e) {
        if (sv != null)
            sv.onError(e);
    };

    public ProfiledTreeStreamVisitor asSkipVisitEnd() {
        return new ProfiledTreeStreamVisitor(api, this) {
            @Override
            public void visitEnd() {
                // No operation
            }

            @Override
            public ProfiledTreeStreamVisitor asSkipVisitEnd() {
                return this;
            }
        };
    }
}
