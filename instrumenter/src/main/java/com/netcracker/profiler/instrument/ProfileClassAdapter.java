package com.netcracker.profiler.instrument;

import static com.netcracker.profiler.instrument.enhancement.EnhancerConstants.OPCODES_VERSION;

import com.netcracker.profiler.util.MethodInstrumentationInfo;

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.MethodVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public class ProfileClassAdapter extends ClassVisitor {
    private final static Logger log = LoggerFactory.getLogger(ProfileClassAdapter.class);
    private final String className;
    private String sourceFileName;
    private final HashMap<String, MethodInstrumentationInfo> selectedRules;
    private final String jarName;
    private int classVersion;

    /**
     * Constructs a new {@link org.objectweb.asm.ClassVisitor} object.
     *
     * @param cv            the class visitor to which this adapter must delegate calls.
     * @param className     JVM class name like java/lang/String
     * @param selectedRules
     * @param jarName
     */
    public ProfileClassAdapter(ClassVisitor cv, String className, HashMap<String, MethodInstrumentationInfo> selectedRules, String jarName) {
        super(OPCODES_VERSION, cv);
        this.className = className;
        this.selectedRules = selectedRules;
        this.jarName = jarName;
        log.debug("Transforming class {}, {} rules match this class name", className, selectedRules.size());
    }

    @Override
    public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
        super.visit(version, access, name, signature, superName, interfaces);
        this.classVersion = version & 0xffff; //Store only major version
    }

    @Override
    public void visitSource(String source, String debug) {
        super.visitSource(source, debug);
        sourceFileName = source;
    }

    @Override
    public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
        final MethodInstrumentationInfo info = selectedRules.get(name + desc);
        MethodVisitor mv = super.visitMethod(access, name, desc, signature, exceptions);
        if (info != null) {
            final String fullName = TypeUtils.getMethodFullname(name, desc, className, sourceFileName, info.firstLineNumber, jarName);
            mv = new ProfileMethodAdapter(mv, access, className, name, desc, fullName, info.rule, classVersion);
        }
        return mv;
    }

    @Override
    public void visitEnd() {
        for (MethodInstrumentationInfo info : selectedRules.values()) {
            info.rule.onClassEnd(this, className);
        }
        super.visitEnd();
    }

    public int getClassVersion() {
        return classVersion;
    }
}
