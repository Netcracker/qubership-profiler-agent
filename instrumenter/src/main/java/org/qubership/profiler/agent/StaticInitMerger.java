package org.qubership.profiler.agent;

import static org.qubership.profiler.instrument.enhancement.EnhancerConstants.OPCODES_VERSION;

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link ClassVisitor} that merges clinit methods into a single one.
 *
 * @author Vladimir Sitnikov
 */
public class StaticInitMerger extends ClassVisitor {
    private final static Logger log = LoggerFactory.getLogger(StaticInitMerger.class);

    private String className;

    private MethodVisitor mergedClinit;

    private final String prefix;

    private int counter;

    private boolean isInterface;

    public StaticInitMerger(final String prefix, final ClassVisitor cv) {
        this(OPCODES_VERSION, prefix, cv);
    }

    protected StaticInitMerger(final int api, final String prefix,
            final ClassVisitor cv) {
        super(api, cv);
        this.prefix = prefix;
    }

    @Override
    public void visit(final int version, final int access, final String name,
            final String signature, final String superName,
            final String[] interfaces) {
        cv.visit(version, access, name, signature, superName, interfaces);
        this.className = name;
        this.isInterface = (access & Opcodes.ACC_INTERFACE) > 0;
    }

    @Override
    public MethodVisitor visitMethod(final int access, final String name,
            final String desc, final String signature, final String[] exceptions) {
        MethodVisitor mv;
        if (!"<clinit>".equals(name)) {
            mv = cv.visitMethod(access, name, desc, signature, exceptions);
        } else {
            int a = Opcodes.ACC_PRIVATE + Opcodes.ACC_STATIC;

            if (mergedClinit == null) {
                log.trace("Adding method {} to class {} that will merge all <clinit>s (if more than one)", prefix, className);
                mergedClinit = cv.visitMethod(a, prefix, "()V", null, null);
                mv = cv.visitMethod(access, name, desc, signature, exceptions);
                mv = new StaticInitReturnPatcher(mv, className, prefix, "()V", isInterface);
            } else {
                String n = prefix + counter++;
                mv = cv.visitMethod(a, n, desc, signature, exceptions);
                log.info("Adding method {} to class {} with code of {}", new Object[]{n, className, name});
                mergedClinit.visitMethodInsn(Opcodes.INVOKESTATIC, className, n, desc, false);
            }
        }
        return mv;
    }

    @Override
    public void visitEnd() {
        if (mergedClinit != null) {
            log.trace("Closing {} method", prefix);
            mergedClinit.visitInsn(Opcodes.RETURN);
            mergedClinit.visitMaxs(0, 0);
            mergedClinit.visitEnd();
        }
        super.visitEnd();
    }
}
