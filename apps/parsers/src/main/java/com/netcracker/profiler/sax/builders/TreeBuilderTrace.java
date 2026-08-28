package com.netcracker.profiler.sax.builders;

import com.netcracker.profiler.io.Hotspot;
import com.netcracker.profiler.io.HotspotTag;
import com.netcracker.profiler.io.SuspendLog;
import com.netcracker.profiler.sax.raw.TreeTraceVisitor;
import com.netcracker.profiler.sax.values.ValueHolder;
import com.netcracker.profiler.util.ProfilerConstants;

import java.util.Arrays;
import java.util.function.Supplier;

public class TreeBuilderTrace extends TreeTraceVisitor implements Supplier<Hotspot> {
    private final Hotspot root;
    protected Hotspot[] callTree = new Hotspot[1000];
    protected Hotspot[] stack = new Hotspot[1000];
    protected HotspotTag.Builder[] tagStack = new HotspotTag.Builder[1000];

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
        if (size < callTree.length) {
            return;
        }
        callTree = Arrays.copyOf(callTree, callTree.length * 2);
        stack = Arrays.copyOf(stack, stack.length * 2);
        tagStack = Arrays.copyOf(tagStack, tagStack.length * 2);
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
        HotspotTag.Builder tagBuilder = tagStack[sp];
        if (tagBuilder != null) {
            tagBuilder.forEachTag((tag) -> {
                tag.count = 1;
                tag.totalTime = hs.totalTime;
                hs.addTag(tag);
            });
            tagBuilder.clear();
        }
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
        HotspotTag.Builder tagBuilder = tagStack[getSp()];
        if (tagBuilder == null) {
            tagBuilder = new HotspotTag.Builder();
            tagStack[getSp()] = tagBuilder;
        }
        tagBuilder.addValue(labelId, value);
    }

    @Override
    public void visitEnd() {
        root.calculateTotalExecutions();
    }

    public Hotspot get() {
        return root;
    }
}
