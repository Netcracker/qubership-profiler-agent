package com.netcracker.profiler.sax.raw;

import com.netcracker.profiler.sax.values.ValueHolder;
import com.netcracker.profiler.util.ProfilerConstants;

/**
 * A visitor to visit profiling event stream:
 *    method enter
 *    method exit
 *    label
 *
 * Methods must be called in the following order
 */
public class TreeTraceVisitor {
    /**
     * The API version implemented by this visitor. The value of this field
     * must be one of {@link ProfilerConstants#PROFILER_V1}.
     */
    protected final int api;

    protected final TreeTraceVisitor tv;

    private long time;
    private int sp;

    public TreeTraceVisitor(int api) {
        this(api, null);
    }

    public TreeTraceVisitor(int api, TreeTraceVisitor tv) {
        this.api = api;
        this.tv = tv;
    }

    public void visitEnter(int methodId) {
        sp++;
        if (tv != null)
            tv.visitEnter(methodId);
    }

    public void visitEnter(int methodId,
                           long lastAssemblyId,
                           long lastParentAssemblyId,
                           byte isReactorEndPoint,
                           byte isReactorFrame,
                           long reactStartTime,
                           int  reactDuration,
                           int blockingOperator,
                           int prevOperation,
                           int currentOperation,
                           int emit){
        sp++;
        if (tv != null)
            tv.visitEnter(methodId,
                         lastAssemblyId,
                         lastParentAssemblyId,
                         isReactorEndPoint,
                         isReactorFrame,
                         reactStartTime,
                         reactDuration,
                         blockingOperator,
                         prevOperation,
                         currentOperation,
                    emit);
    }

    public void visitExit() {
        if (tv != null)
            tv.visitExit();
        sp--;
    }

    public void visitLabel(int labelId, ValueHolder value) {
        if (tv != null)
            tv.visitLabel(labelId, value);
    }

    public void visitLabel(int labelId, ValueHolder value, long assemblyId) {
        if (tv != null)
            tv.visitLabel(labelId, value, assemblyId);
    }

    public void visitTimeAdvance(long timeAdvance) {
        time += timeAdvance;
        if (tv != null)
            tv.visitTimeAdvance(timeAdvance);
    }

    public void visitEnd() {
        if (tv != null)
            tv.visitEnd();
    }

    public int getSp() {
        return sp;
    }

    public long getTime() {
        return time;
    }

    public TreeTraceVisitor asSkipVisitEnd() {
        return new TreeTraceVisitor(api, this) {
            @Override
            public void visitEnd() {
                // No operation
            }

            @Override
            public TreeTraceVisitor asSkipVisitEnd() {
                return this;
            }
        };
    }
}
