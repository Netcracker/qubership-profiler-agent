package org.qubership.profiler.analyzer;

import org.qubership.profiler.dom.ProfiledTree;
import org.qubership.profiler.dom.ProfiledTreeStreamVisitor;
import org.qubership.profiler.io.exceptions.ErrorSupervisor;
import org.qubership.profiler.util.ProfilerConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MergeTrees extends ProfiledTreeStreamVisitor {
    public static final Logger log = LoggerFactory.getLogger(MergeTrees.class);
    private ProfiledTree tree;

    public MergeTrees(ProfiledTreeStreamVisitor tv) {
        this(ProfilerConstants.PROFILER_V1, tv);
    }

    protected MergeTrees(int api, ProfiledTreeStreamVisitor tv) {
        super(api, tv);
    }

    @Override
    public void visitTree(ProfiledTree tree) {
        if (this.tree == null)
            this.tree = tree;
        else
            this.tree.merge(tree);
    }

    @Override
    public void visitEnd() {
        if (tree != null)
            super.visitTree(tree);
        else
            ErrorSupervisor.getInstance().warn("MergeTrees finishes with no actual tree. Looks like call to #visitTree was missing", null);
        super.visitEnd();
    }
}
