package com.netcracker.profiler.sax.builders;

import com.netcracker.profiler.chart.Provider;
import com.netcracker.profiler.io.Hotspot;
import com.netcracker.profiler.io.SuspendLog;
import com.netcracker.profiler.sax.raw.TreeTraceVisitor;
import com.netcracker.profiler.sax.values.ValueHolder;
import com.netcracker.profiler.util.ProfilerConstants;

public class TreeBuilderTrace extends TreeTraceVisitor implements Provider<Hotspot> {
    private final Hotspot root;
    protected Hotspot[] callTree = new Hotspot[1000];
    protected Hotspot[] stack = new Hotspot[1000];

    private final SuspendLog suspendLog;
    private final SuspendLog.SuspendLogCursor suspendCursor;

    public TreeBuilderTrace(Hotspot root, SuspendLog suspendLog) {
        super(ProfilerConstants.PROFILER_V1);
        this.root = root;
        callTree[0] = root;
        stack[0] = new Hotspot(-1);
        this.suspendLog = suspendLog;
        this.suspendCursor = this.suspendLog.cursor();
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

    @Override
    public void visitEnter(int methodId) {
        long time = getTime();
        int sp = getSp();

        Hotspot callTreeParent = callTree[sp];
        if (sp != 0)
            callTreeParent.suspensionTime += suspendCursor.moveTo(time);
        else {
            suspendCursor.skipTo(time);
            callTree[0].startTime = Math.min(callTree[0].startTime, time);
        }
        super.visitEnter(methodId);
        sp++;
        ensureStorage(sp);
        Hotspot orCreateChild = callTreeParent.getOrCreateChild(methodId);

        callTree[sp] = orCreateChild;
        Hotspot hs = stack[sp] = new Hotspot(methodId);
        hs.startTime = time;
        hs.endTime = time;
        hs.totalTime = (int) -time;
    }

    @Override
    public void visitExit() {
        long time = getTime();
        int sp = getSp();
        Hotspot hs = stack[sp];
        hs.suspensionTime += suspendCursor.moveTo(time);
        hs.totalTime += (int) time;
        hs.count++;
        callTree[sp].merge(hs);
        super.visitExit();
        sp--;
        if (sp != 0)
            return;
        callTree[0].endTime = Math.max(callTree[0].endTime, time);
        callTree[0].count++;
    }

    @Override
    public void visitLabel(int labelId, ValueHolder value) {
        stack[getSp()].tag(labelId, value);
    }

    @Override
    public void visitEnd() {
        root.calculateTotalExecutions();
    }

    public Hotspot get() {
        return root;
    }
}
