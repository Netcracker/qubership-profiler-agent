package com.netcracker.cdt.ui.services.tree.context;

import com.netcracker.cdt.ui.services.tree.data.ProfiledTree;
import com.netcracker.common.models.meta.ClobIndex;
import com.netcracker.common.models.meta.DictionaryIndex;
import com.netcracker.cdt.ui.services.tree.data.Hotspot;
import com.netcracker.common.models.SuspendRange;
import com.netcracker.profiler.model.TreeRowId;
import com.netcracker.common.models.meta.Value;

/**
 * A visitor to visit profiling event stream:
 *    method enter
 *    method exit
 *    label
 * Methods must be called in the following order
 */
public class TreeTraceBuilder {
    private final ProfiledTree tree;
    private final Hotspot root;
    private final SuspendRange.Cursor suspendCursor;

    public static TreeTraceBuilder create(DictionaryIndex dict, SuspendRange suspendLog, ClobIndex clobValues, TreeRowId rowid) {
        final ProfiledTree tree = new ProfiledTree(dict, clobValues, rowid);
        Hotspot root = tree.getRoot();
        root.fullRowId = rowid.fullRowId;
        root.folderId = rowid.folderId;
        return new TreeTraceBuilder(suspendLog, tree, root);
    }

    protected Hotspot[] callTree = new Hotspot[1000];
    protected Hotspot[] stack = new Hotspot[1000];
    public boolean started;
    private long time;
    private int sp;

    public TreeTraceBuilder(SuspendRange suspendLog, ProfiledTree tree, Hotspot root) {
        this.tree = tree;
        this.root = root;
        this.suspendCursor = suspendLog.cursor();
        callTree[0] = root;
        stack[0] = new Hotspot(-1);
    }

    public ProfiledTree getTree() {
        return tree;
    }

    public long getTime() {
        return time;
    }

    public int getSp() {
        return sp;
    }

    protected void ensureStorage(int size) {
        if (size < callTree.length)
            return;
        Hotspot[] tmp = new Hotspot[callTree.length * 2];
        System.arraycopy(callTree, 0, tmp, 0, callTree.length);
        callTree = tmp;

        tmp = new Hotspot[stack.length * 2];
        System.arraycopy(stack, 0, tmp, 0, stack.length);
        stack = tmp;
    }


    public void visitEnter(int methodId) {
        long time = getTime();

        Hotspot callTreeParent = callTree[sp];
        if (sp != 0)
            callTreeParent.suspensionTime += suspendCursor.moveTo(time);
        else {
            suspendCursor.skipTo(time);
            callTree[0].startTime = Math.min(callTree[0].startTime, time);
        }
        sp++;
        ensureStorage(sp);
        Hotspot orCreateChild = callTreeParent.getOrCreateChild(methodId);

        callTree[sp] = orCreateChild;
        Hotspot hs = stack[sp] = new Hotspot(methodId);
        hs.startTime = time;
        hs.endTime = time;
        hs.totalTime = (int) -time;
    }

    public void visitExit() {
        long time = getTime();
        Hotspot hs = stack[sp];
        hs.suspensionTime += suspendCursor.moveTo(time);
        hs.totalTime += (int) time;
        hs.count++;
        callTree[sp].merge(hs);
        sp--;
        if (sp != 0)
            return;
        callTree[0].endTime = Math.max(callTree[0].endTime, time);
        callTree[0].count++;
    }


    public void visitLabel(int labelId, Value value, long assemblyId) {
        stack[getSp()].tag(0, labelId, 0, value, assemblyId);
    }

    public void visitLabel(int labelId, Value value) {
        visitLabel(labelId, value, 0);
    }

    public void visitEnd() {
        root.calculateTotalExecutions();
    }

    public void visitTimeAdvance(long timeAdvance) {
        time += timeAdvance;
    }
    public Hotspot get() {
        return root;
    }
}
