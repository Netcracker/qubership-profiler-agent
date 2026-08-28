package com.netcracker.profiler.instrument.enhancement;

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Opcodes;

public interface ClassEnhancer extends Opcodes {
    public void enhance(ClassVisitor cv);
}
