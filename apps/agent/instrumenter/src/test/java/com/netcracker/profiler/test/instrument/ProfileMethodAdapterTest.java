package com.netcracker.profiler.test.instrument;

import static com.netcracker.profiler.instrument.enhancement.EnhancerConstants.OPCODES_VERSION;

import com.netcracker.profiler.configuration.Rule;
import com.netcracker.profiler.instrument.ProfileMethodAdapter;
import com.netcracker.profiler.instrument.TypeUtils;

import org.junit.jupiter.api.Test;
import org.objectweb.asm.*;
import org.objectweb.asm.util.ASMifier;
import org.objectweb.asm.util.CheckClassAdapter;
import org.objectweb.asm.util.TraceClassVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;

public class ProfileMethodAdapterTest implements Opcodes {
    private static final Logger log = LoggerFactory.getLogger(ProfileMethodAdapterTest.class);

    public ClassVisitor createVerifier(ClassVisitor out) {
        ClassVisitor profilingChain = new ClassVisitor(OPCODES_VERSION, out) {
            String className;
            int classVersion;
            @Override
            public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
                super.visit(version, access, name, signature, superName, interfaces);
                className = name;
                classVersion = version;
            }

            @Override
            public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
                Rule rule = new Rule();
                final String fullName = TypeUtils.getMethodFullname(name, desc, className,
                        "test.java", 42, "test.jar");
                MethodVisitor mv = super.visitMethod(access, name, desc, signature, exceptions);
//                return mv;
                return new ProfileMethodAdapter(mv, access, className, name, desc, fullName, rule, classVersion);
            }
        };
        return profilingChain;
    }

    void verify(byte[] bytes) {
        ClassVisitor out = new ClassWriter(0);
        ClassVisitor check = new CheckClassAdapter(out);
        final ASMifier asm = new ASMifier();
        TraceClassVisitor printer = new TraceClassVisitor(check, asm, null);
        ClassReader cr = new ClassReader(bytes);
        cr.accept(printer, 0);
        if (log.isErrorEnabled()) {
            StringWriter sw = new StringWriter();
            try(PrintWriter pw = new PrintWriter(sw)) {
                asm.print(pw);
            }
            log.debug("Class body: {}", sw);
        }
    }

    @Test
    public void strangeConstructor() {
        ClassWriter out = new ClassWriter(0);
        ClassVisitor cw = createVerifier(out);
        MethodVisitor mv;

        cw.visit(V1_2, ACC_PUBLIC, "com/netcracker/applications/nso/catalog/impl/ConstituentVNFDescriptorManagerImpl$$EnhancerBySpringCGLIB$$7578559e", null, "com/netcracker/applications/nso/catalog/impl/ConstituentVNFDescriptorManagerImpl", new String[] { "org/springframework/aop/SpringProxy", "org/springframework/aop/framework/Advised", "org/springframework/cglib/proxy/Factory" });
        {
            mv = cw.visitMethod(ACC_PUBLIC, "<init>", "(Lcom/netcracker/applications/nso/validators/NSOValidator;Lcom/netcracker/applications/nso/entitymanager/EntityManagerAdapter;Lcom/netcracker/applications/nso/catalog/converters/ConstituentVNFDescriptorConverter;Lcom/netcracker/applications/nso/catalog/impl/NsoCreationService;Lcom/netcracker/framework/jdbc/JDBCTemplates;Lcom/netcracker/applications/nso/ncdo/caches/CacheHelper;Lcom/netcracker/applications/nso/catalog/impl/NsoUpdateService;Lcom/netcracker/applications/nso/catalog/impl/NsoFinder;)V", null, null);
            mv.visitCode();
            Label l0 = new Label();
            Label l1 = new Label();
            mv.visitTryCatchBlock(l0, l1, l1, "java/lang/RuntimeException");
            mv.visitTryCatchBlock(l0, l1, l1, "java/lang/Error");
            Label l2 = new Label();
            mv.visitTryCatchBlock(l0, l1, l2, "java/lang/Throwable");
            Label l3 = new Label();
            Label l4 = new Label();
            mv.visitTryCatchBlock(l3, l4, l4, "java/lang/Throwable");
            mv.visitLabel(l0);
            mv.visitVarInsn(ALOAD, 0);
            mv.visitInsn(DUP);
            mv.visitVarInsn(ALOAD, 1);
            mv.visitVarInsn(ALOAD, 2);
            mv.visitVarInsn(ALOAD, 3);
            mv.visitVarInsn(ALOAD, 4);
            mv.visitVarInsn(ALOAD, 5);
            mv.visitVarInsn(ALOAD, 6);
            mv.visitVarInsn(ALOAD, 7);
            mv.visitVarInsn(ALOAD, 8);
            mv.visitMethodInsn(INVOKESPECIAL, "com/netcracker/applications/nso/catalog/impl/ConstituentVNFDescriptorManagerImpl", "<init>", "(Lcom/netcracker/applications/nso/validators/NSOValidator;Lcom/netcracker/applications/nso/entitymanager/EntityManagerAdapter;Lcom/netcracker/applications/nso/catalog/converters/ConstituentVNFDescriptorConverter;Lcom/netcracker/applications/nso/catalog/impl/NsoCreationService;Lcom/netcracker/framework/jdbc/JDBCTemplates;Lcom/netcracker/applications/nso/ncdo/caches/CacheHelper;Lcom/netcracker/applications/nso/catalog/impl/NsoUpdateService;Lcom/netcracker/applications/nso/catalog/impl/NsoFinder;)V", false);
            mv.visitLabel(l3);
            mv.visitMethodInsn(INVOKESTATIC, "com/netcracker/applications/nso/catalog/impl/ConstituentVNFDescriptorManagerImpl$$EnhancerBySpringCGLIB$$7578559e", "CGLIB$BIND_CALLBACKS", "(Ljava/lang/Object;)V", false);
            mv.visitInsn(RETURN);
            mv.visitLabel(l1);
            mv.visitFrame(Opcodes.F_NEW, 9, new Object[] {Opcodes.TOP, "com/netcracker/applications/nso/validators/NSOValidator", "com/netcracker/applications/nso/entitymanager/EntityManagerAdapter", "com/netcracker/applications/nso/catalog/converters/ConstituentVNFDescriptorConverter", "com/netcracker/applications/nso/catalog/impl/NsoCreationService", "com/netcracker/framework/jdbc/JDBCTemplates", "com/netcracker/applications/nso/ncdo/caches/CacheHelper", "com/netcracker/applications/nso/catalog/impl/NsoUpdateService", "com/netcracker/applications/nso/catalog/impl/NsoFinder"}, 1, new Object[] {"java/lang/Throwable"});
            mv.visitInsn(ATHROW);
            mv.visitLabel(l2);
            mv.visitFrame(Opcodes.F_NEW, 9, new Object[] {Opcodes.TOP, "com/netcracker/applications/nso/validators/NSOValidator", "com/netcracker/applications/nso/entitymanager/EntityManagerAdapter", "com/netcracker/applications/nso/catalog/converters/ConstituentVNFDescriptorConverter", "com/netcracker/applications/nso/catalog/impl/NsoCreationService", "com/netcracker/framework/jdbc/JDBCTemplates", "com/netcracker/applications/nso/ncdo/caches/CacheHelper", "com/netcracker/applications/nso/catalog/impl/NsoUpdateService", "com/netcracker/applications/nso/catalog/impl/NsoFinder"}, 1, new Object[] {"java/lang/Throwable"});
            mv.visitTypeInsn(NEW, "java/lang/reflect/UndeclaredThrowableException");
            mv.visitInsn(DUP_X1);
            mv.visitInsn(SWAP);
            mv.visitMethodInsn(INVOKESPECIAL, "java/lang/reflect/UndeclaredThrowableException", "<init>", "(Ljava/lang/Throwable;)V", false);
            mv.visitInsn(ATHROW);
            mv.visitLabel(l4);
            mv.visitFrame(Opcodes.F_NEW, 9, new Object[] {"com/netcracker/applications/nso/catalog/impl/ConstituentVNFDescriptorManagerImpl$$EnhancerBySpringCGLIB$$7578559e", "com/netcracker/applications/nso/validators/NSOValidator", "com/netcracker/applications/nso/entitymanager/EntityManagerAdapter", "com/netcracker/applications/nso/catalog/converters/ConstituentVNFDescriptorConverter", "com/netcracker/applications/nso/catalog/impl/NsoCreationService", "com/netcracker/framework/jdbc/JDBCTemplates", "com/netcracker/applications/nso/ncdo/caches/CacheHelper", "com/netcracker/applications/nso/catalog/impl/NsoUpdateService", "com/netcracker/applications/nso/catalog/impl/NsoFinder"}, 1, new Object[] {"java/lang/Throwable"});
            mv.visitVarInsn(ASTORE, 9);
            mv.visitVarInsn(ALOAD, 9);
            mv.visitInsn(ATHROW);
            mv.visitMaxs(10, 10);
            mv.visitEnd();
        }
        verify(out.toByteArray());
    }

    @Test
    public void strangeConstructor2() {
        ClassWriter out = new ClassWriter(0);
        ClassVisitor cw = createVerifier(out);
        MethodVisitor mv;

        cw.visit(V1_2, ACC_PUBLIC, "com/test/Test$$EnhancerBySpringCGLIB$$7578559e", null, "com/test/TestImpl", new String[] { "org/springframework/aop/SpringProxy"});
        {
            mv = cw.visitMethod(ACC_PUBLIC, "<init>", "()V", null, null);
            mv.visitCode();
            Label l0 = new Label();
            Label l1 = new Label();
            mv.visitTryCatchBlock(l0, l1, l1, "java/lang/Throwable");
            Label l3 = new Label();
            Label l4 = new Label();
            mv.visitTryCatchBlock(l3, l4, l4, "java/lang/Throwable");
            mv.visitLabel(l0);
            mv.visitVarInsn(ALOAD, 0);
            mv.visitInsn(DUP);
            mv.visitMethodInsn(INVOKESPECIAL, "com/test/TestImpl", "<init>", "()V", false);
            mv.visitLabel(l3);
            mv.visitMethodInsn(INVOKESTATIC, "com/test/Test$$EnhancerBySpringCGLIB$$7578559e", "CGLIB$BIND_CALLBACKS", "(Ljava/lang/Object;)V", false);
            mv.visitInsn(RETURN);
            mv.visitLabel(l1);
            mv.visitFrame(Opcodes.F_NEW, 1, new Object[] {Opcodes.TOP}, 1, new Object[] {"java/lang/Throwable"});
            mv.visitInsn(ATHROW);
            mv.visitLabel(l4);
            mv.visitFrame(Opcodes.F_NEW, 1, new Object[] {"com/test/Test$$EnhancerBySpringCGLIB$$7578559e"}, 1, new Object[] {"java/lang/Throwable"});
            mv.visitVarInsn(ASTORE, 1);
            mv.visitVarInsn(ALOAD, 1);
            mv.visitInsn(ATHROW);
            mv.visitMaxs(3, 2);
            mv.visitEnd();
        }
        verify(out.toByteArray());
    }
}
