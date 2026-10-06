package com.netcracker.profiler.instrument.custom.util;

import static com.netcracker.profiler.instrument.enhancement.EnhancerConstants.OPCODES_VERSION;

import com.netcracker.profiler.agent.DefaultMethodImplInfo;
import com.netcracker.profiler.instrument.TypeUtils;

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultMethodAdder extends ClassVisitor {
    private final static Logger log = LoggerFactory.getLogger(DefaultMethodAdder.class);

    private final DefaultMethodImplInfo methodInfo;
    private boolean methodAlreadyExists;
    private String superName;
    private boolean isInterface;

    public DefaultMethodAdder(ClassVisitor cv, DefaultMethodImplInfo methodInfo) {
        super(OPCODES_VERSION, cv);
        this.methodInfo = methodInfo;
    }

    @Override
    public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
        this.superName = superName;
        isInterface = (access & Opcodes.ACC_INTERFACE) > 0;
        super.visit(version, access, name, signature, superName, interfaces);
    }

    @Override
    public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
        if (methodInfo.methodName.equals(name) && desc.equals(methodInfo.methodDescr))
            methodAlreadyExists = true;

        return super.visitMethod(access, name, desc, signature, exceptions);
    }

    @Override
    public void visitEnd() {
        if (!methodAlreadyExists) {
            log.info("Adding method {}{} to class {}", new Object[]{methodInfo.methodName, methodInfo.methodDescr, methodInfo.className});
            MethodVisitor mv = visitMethod(methodInfo.access, methodInfo.methodName, methodInfo.methodDescr, null, null);
            GeneratorAdapter ga = new GeneratorAdapter(mv, methodInfo.access, methodInfo.methodName, methodInfo.methodDescr);
            mv.visitCode();
            if (methodInfo.skipSuper) {
                Type returnType = Type.getReturnType(methodInfo.methodDescr);
                TypeUtils.pushDefaultValue(ga, returnType);
            } else {
                ga.loadThis();
                ga.loadArgs();
                mv.visitMethodInsn(Opcodes.INVOKESPECIAL, superName, methodInfo.methodName, methodInfo.methodDescr, isInterface);
            }
            ga.returnValue();
            int argsLength = 1; // this
            Type[] argumentTypes = Type.getArgumentTypes(methodInfo.methodDescr);
            for (Type argumentType : argumentTypes) {
                argsLength += argumentType.getSize();
            }
            ga.visitMaxs(argsLength, argsLength);
            mv.visitEnd();
        }
        super.visitEnd();
    }
}
