package org.qubership.profiler.agent;

import static org.qubership.profiler.instrument.enhancement.EnhancerConstants.OPCODES_VERSION;

import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

/**
 * Inserts calls of specific method to predefined method.
 * The goal is to add call to the clinit$merger$profiler method to the end of default static initializer
 */
public class StaticInitReturnPatcher extends MethodVisitor {
    private final String owner;
    private final String methodName;
    private final String methodDescriptor;
    private final boolean isInterface;

    public StaticInitReturnPatcher(MethodVisitor mv, String owner, String methodName, String methodDescriptor, boolean isInterface) {
        super(OPCODES_VERSION, mv);
        this.owner = owner;
        this.methodName = methodName;
        this.methodDescriptor = methodDescriptor;
        this.isInterface = isInterface;
    }

    @Override
    public void visitInsn(int opcode) {
        if (opcode == Opcodes.RETURN) {
            visitMethodInsn(Opcodes.INVOKESTATIC, owner, methodName, methodDescriptor, isInterface);
        }
        super.visitInsn(opcode);
    }
}
