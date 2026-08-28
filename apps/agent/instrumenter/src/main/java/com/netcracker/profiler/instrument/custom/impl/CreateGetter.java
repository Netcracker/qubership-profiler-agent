package com.netcracker.profiler.instrument.custom.impl;

import com.netcracker.profiler.agent.Configuration;
import com.netcracker.profiler.instrument.ProfileClassAdapter;

import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.w3c.dom.Element;

public class CreateGetter extends ClassInstrumenter implements Opcodes {
    private String field;
    private String type;

    public void onClass(ProfileClassAdapter ca, String className) {
        final MethodVisitor mv = ca.visitMethod(ACC_PUBLIC, "profiler$get_" + field, "()" + type, null, null);
        mv.visitVarInsn(ALOAD, 0);
        mv.visitFieldInsn(GETFIELD, className, field, type);
        mv.visitInsn(Type.getObjectType(type).getOpcode(IRETURN));
        mv.visitMaxs(0, 0);
    }

    @Override
    public ClassInstrumenter init(Element e, Configuration configuration) {
        field = e.getAttribute("field");
        type = e.getAttribute("type");
        return this;
    }
}
