package org.qubership.profiler.sax.builders;

import org.qubership.profiler.dom.ClobValues;
import org.qubership.profiler.dom.ProfiledTree;
import org.qubership.profiler.dom.ProfiledTreeStreamVisitor;
import org.qubership.profiler.dom.TagDictionary;
import org.qubership.profiler.io.Hotspot;
import org.qubership.profiler.io.SuspendLog;
import org.qubership.profiler.sax.raw.TraceVisitor;
import org.qubership.profiler.sax.raw.TreeRowid;
import org.qubership.profiler.util.ProfilerConstants;

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
