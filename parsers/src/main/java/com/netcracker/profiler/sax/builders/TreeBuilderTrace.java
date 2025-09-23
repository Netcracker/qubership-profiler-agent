package com.netcracker.profiler.sax.builders;

import com.netcracker.profiler.chart.Provider;
import com.netcracker.profiler.io.Hotspot;
import com.netcracker.profiler.io.SuspendLog;
import com.netcracker.profiler.sax.raw.TreeTraceVisitor;
import com.netcracker.profiler.sax.values.ValueHolder;
import com.netcracker.profiler.util.ProfilerConstants;

import java.util.HashSet;

public class TreeBuilderTrace extends TreeTraceVisitor implements Provider<Hotspot> {
    private final Hotspot root;
    protected Hotspot[] callTree = new Hotspot[1000];
    protected Hotspot[] stack = new Hotspot[1000];

    public boolean started;
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
    public void visitEnter(int methodId,
                           long lastAssemblyId,
                           long lastParentAssemblyId,
                           byte isReactorEndPoint,
                           byte isReactorFrame,
                           long reactorStartTime,
                           int  reactorDuration,
                           int blockingOperator,
                           int prevOperation,
                           int currentOperation,
                           int emit) {
        long time = getTime();
        int sp = getSp();

        Hotspot callTreeParent = callTree[sp];
        if (sp != 0)
            callTreeParent.suspensionTime += suspendCursor.moveTo(time);
        else {
            suspendCursor.skipTo(time);
            callTree[0].startTime = Math.min(callTree[0].startTime, time);
        }
        super.visitEnter(methodId, lastAssemblyId, lastParentAssemblyId,
                isReactorEndPoint, isReactorFrame,
                reactorStartTime, reactorDuration,
                blockingOperator, prevOperation, currentOperation,
                emit);
        sp++;
        ensureStorage(sp);
        Hotspot orCreateChild = callTreeParent.getOrCreateChild(methodId, lastParentAssemblyId);
        propagateReactorParams(lastAssemblyId, lastParentAssemblyId, isReactorEndPoint,
                isReactorFrame, blockingOperator, prevOperation, currentOperation,
                emit, orCreateChild);

        callTree[sp] = orCreateChild;
        Hotspot hs = stack[sp] = new Hotspot(methodId);
        hs.startTime = time;
        hs.endTime = time;
        hs.totalTime = (int) -time;
        if (reactorDuration != 0 && blockingOperator != 0) {
            orCreateChild.reactorDuration = reactorDuration;
            orCreateChild.reactorStartTime = reactorStartTime;
            hs.reactorDuration = reactorDuration;
            hs.reactorStartTime = reactorStartTime;
        }
    }

    private void propagateReactorParams(long lastAssemblyId,
                                        long lastParentAssemblyId,
                                        byte isReactorEndPoint,
                                        byte isReactorFrame,
                                        int blockingOperator,
                                        int prevOperation,
                                        int currentOperation,
                                        int emit,
                                        Hotspot orCreateChild) {
        orCreateChild.isReactorEndPoint = isReactorEndPoint;
        orCreateChild.isReactorFrame = isReactorFrame;
        if (lastAssemblyId != 0) {
            if (orCreateChild.lastAssemblyId == null) {
                orCreateChild.lastAssemblyId = new HashSet<>();
            }
            orCreateChild.lastAssemblyId.add(lastAssemblyId);
        }

        if (lastParentAssemblyId != 0) {
            orCreateChild.lastParentAssemblyId = lastParentAssemblyId;
        }

        if (blockingOperator != 0) {
            orCreateChild.blockingOperator = blockingOperator;
        }

        if (prevOperation != 0) {
            orCreateChild.prevOperation = prevOperation;
        }

        if (currentOperation != 0) {
            orCreateChild.currentOperation = currentOperation;
        }

        if (emit != 0) {
            orCreateChild.emit = emit;
        }
    }

    @Override
    public void visitEnter(int methodId) {
        visitEnter(methodId, 0, 0, (byte) 0, (byte) 0,
                0, 0, 0, 0,
                0, 0);
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
    public void visitLabel(int labelId, ValueHolder value, long assemblyId) {
        stack[getSp()].tag(0, labelId, 0, value, assemblyId);
    }

    @Override
    public void visitLabel(int labelId, ValueHolder value) {
        visitLabel(labelId, value, 0);
    }

    @Override
    public void visitEnd() {
        root.calculateTotalExecutions();
    }

    public Hotspot get() {
        return root;
    }
}
