package com.netcracker.profiler.sax.visitors;

public interface ISuspendLogVisitor {

    void visitHiccup(long date, int delay);

    void visitEnd();
}
