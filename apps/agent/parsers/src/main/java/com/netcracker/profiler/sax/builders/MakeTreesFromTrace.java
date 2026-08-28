package com.netcracker.profiler.sax.builders;

import com.netcracker.profiler.dom.ClobValues;
import com.netcracker.profiler.dom.ProfiledTree;
import com.netcracker.profiler.dom.ProfiledTreeStreamVisitor;
import com.netcracker.profiler.dom.TagDictionary;
import com.netcracker.profiler.io.Hotspot;
import com.netcracker.profiler.io.SuspendLog;
import com.netcracker.profiler.sax.raw.TraceVisitor;
import com.netcracker.profiler.sax.raw.TreeRowid;
import com.netcracker.profiler.util.ProfilerConstants;

public class MakeTreesFromTrace extends TraceVisitor {
    private final ProfiledTreeStreamVisitor sv;
    private final TagDictionary dict;
    private final SuspendLog suspendLog;
    private final ClobValues clobValues;

    public MakeTreesFromTrace(ProfiledTreeStreamVisitor sv, TagDictionary dict, SuspendLog suspendLog, ClobValues clobValues) {
        this(ProfilerConstants.PROFILER_V1, sv, dict, suspendLog, clobValues);
    }

    protected MakeTreesFromTrace(int api, ProfiledTreeStreamVisitor sv, TagDictionary dict, SuspendLog suspendLog, ClobValues clobValues) {
        super(api);
        this.sv = sv;
        this.dict = dict;
        this.suspendLog = suspendLog;
        this.clobValues = clobValues;
    }

    @Override
    public TreeBuilderTrace visitTree(TreeRowid rowid) {
        final ProfiledTree tree = new ProfiledTree(dict, clobValues, rowid);
        Hotspot root = tree.getRoot();
        root.fullRowId = rowid.fullRowId;
        root.folderId = rowid.folderId;
        return new TreeBuilderTrace(root, suspendLog) {
            @Override
            public void visitEnd() {
                super.visitEnd();
                sv.visitTree(tree);
            }
        };
    }

    @Override
    public void visitEnd() {
        sv.visitEnd();
    }
}
