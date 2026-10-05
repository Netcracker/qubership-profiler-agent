package com.netcracker.profiler.test.rules;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.netcracker.profiler.agent.ProfilingTransformer;
import com.netcracker.profiler.agent.plugins.EnhancerRegistryPluginImpl;
import com.netcracker.profiler.configuration.ConfigurationImpl;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Fails when {@code <if-class-declares>} or {@code <if-class-does-not-declare>} does not decide
 * whether a rule instruments the class being transformed.
 *
 * <p>Both tags reach the transformer through two steps nothing else covers: the parser has to map
 * each tag to the list the rule keeps it in, and {@code ProfilingTransformer} has to read the
 * methods of the class under transformation and drop the rules the class does not satisfy. Only the
 * plugin instrumentation tests exercise that path today, and they resolve six third-party
 * distributions to do it; these cases run in one process over a class the test writes itself.</p>
 */
public class ClassStructureRuleTest {
    private static final String SAMPLE = "com/example/Sample";
    private static final String PUBLISH_STRING = "publish(Ljava/lang/String;)V";
    private static final String PROFILER = "com/netcracker/profiler/agent/Profiler";

    @TempDir
    Path directory;

    @Test
    public void aRuleIsKeptWhenTheClassDeclaresTheMethodItRequires() throws Exception {
        assertTrue(profiles(publishRule("<if-class-declares>publish(java.nio.ByteBuffer)</if-class-declares>"), true),
                "if-class-declares publish(ByteBuffer), class declares both");
    }

    @Test
    public void aRuleIsDroppedWhenTheClassLacksTheMethodItRequires() throws Exception {
        assertFalse(profiles(publishRule("<if-class-declares>publish(java.nio.ByteBuffer)</if-class-declares>"), false),
                "if-class-declares publish(ByteBuffer), class declares publish(String) alone");
    }

    @Test
    public void aRuleIsKeptWhenTheClassLacksTheMethodItForbids() throws Exception {
        assertTrue(
                profiles(publishRule("<if-class-does-not-declare>publish(java.nio.ByteBuffer)</if-class-does-not-declare>"), false),
                "if-class-does-not-declare publish(ByteBuffer), class declares publish(String) alone");
    }

    @Test
    public void aRuleIsDroppedWhenTheClassDeclaresTheMethodItForbids() throws Exception {
        assertFalse(
                profiles(publishRule("<if-class-does-not-declare>publish(java.nio.ByteBuffer)</if-class-does-not-declare>"), true),
                "if-class-does-not-declare publish(ByteBuffer), class declares both");
    }

    /**
     * A rule that names no method is collected as one that matches every method of the class, which
     * ends the scan of the remaining rules. A structure criterion is answered from the bytes of the
     * class, which the scan does not have, so a rule carrying one may still be dropped afterwards:
     * ending the scan on it discards the rules written after it and leaves the class with none.
     */
    @Test
    public void aRuleAfterADroppedStructureConditionalRuleStillReachesTheClass() throws Exception {
        assertTrue(profiles(BYTE_BUFFER_WIDE_RULE + publishRule(""), false),
                "publish(String) is profiled on a class the wide rule before it does not match");
    }

    /** The control: the wide rule survives its criterion and claims publish(String) itself. */
    @Test
    public void aStructureConditionalRuleThatSurvivesStillClaimsTheClass() throws Exception {
        assertTrue(profiles(BYTE_BUFFER_WIDE_RULE + publishRule(""), true),
                "publish(String) is profiled on a class the wide rule before it matches");
    }

    /**
     * A conditional exclusion has to reach the transformer for its condition to be answered, and
     * {@code ProfileMethodAdapter} then honors it: the exclusion is selected first, so the rule
     * written after it does not profile the class.
     */
    @Test
    public void aStructureConditionalExclusionThatMatchesStillSuppressesTheRulesAfterIt() throws Exception {
        assertFalse(profiles(SUPPRESS_ON_BYTE_BUFFER + publishRule(""), true),
                "publish(String) is profiled on a class the exclusion before it matches");
    }

    @Test
    public void aStructureConditionalExclusionThatIsDroppedSuppressesNothing() throws Exception {
        assertTrue(profiles(SUPPRESS_ON_BYTE_BUFFER + publishRule(""), false),
                "publish(String) is profiled on a class the exclusion before it does not match");
    }

    private static final String SUPPRESS_ON_BYTE_BUFFER = "<rule>\n"
            + "  <class>com.example.Sample</class>\n"
            + "  <if-class-declares>publish(java.nio.ByteBuffer)</if-class-declares>\n"
            + "  <do-not-profile/>\n"
            + "</rule>\n";

    private static final String BYTE_BUFFER_WIDE_RULE = "<rule>\n"
            + "  <class>com.example.Sample</class>\n"
            + "  <if-class-declares>publish(java.nio.ByteBuffer)</if-class-declares>\n"
            + "</rule>\n";

    private static String publishRule(String criterion) {
        return "<rule>\n"
                + "  <class>com.example.Sample</class>\n"
                + "  " + criterion + "\n"
                + "  <method>publish(java.lang.String)</method>\n"
                + "</rule>\n";
    }

    /**
     * Whether the configuration in {@code rules} leaves {@code publish(String)} profiled.
     *
     * <p>The question is not whether the body was rewritten. {@code ProfileMethodAdapter} rewrites
     * a method it was built for whatever the rule says, and only the {@code Profiler.enterReturning}
     * call it emits is guarded by {@code Rule.shouldNotProfile}, so a body that grew under a
     * {@code <do-not-profile/>} rule is not a profiled one.</p>
     */
    private boolean profiles(String rules, boolean declaresByteBufferOverload) throws Exception {
        byte[] original = sampleClass(declaresByteBufferOverload);
        // ConfigurationImpl reads the enhancer map through Bootstrap, which this registers.
        new EnhancerRegistryPluginImpl();
        ProfilingTransformer transformer = new ProfilingTransformer(new ConfigurationImpl(write(rules).toString()));
        byte[] transformed = transformer.transform(null, SAMPLE, null, null, original);
        return transformed != null && entersTheProfiler(transformed);
    }

    private Path write(String rules) throws IOException {
        Path target = directory.resolve("rules.xml");
        Files.write(target, ("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                + "<profiler-configuration>\n<ruleset>\n" + rules + "</ruleset>\n</profiler-configuration>\n")
                .getBytes(UTF_8));
        return target;
    }

    private static boolean entersTheProfiler(byte[] classFile) {
        ClassNode node = new ClassNode();
        new ClassReader(classFile).accept(node, ClassReader.SKIP_FRAMES);
        for (MethodNode method : node.methods) {
            if (!PUBLISH_STRING.equals(method.name + method.desc))
                continue;
            for (AbstractInsnNode instruction : method.instructions)
                if (instruction instanceof MethodInsnNode) {
                    MethodInsnNode call = (MethodInsnNode) instruction;
                    if (PROFILER.equals(call.owner) && "enterReturning".equals(call.name))
                        return true;
                }
            return false;
        }
        throw new AssertionError(PUBLISH_STRING + " is missing from the transformed " + SAMPLE);
    }

    private static byte[] sampleClass(boolean withByteBufferOverload) {
        ClassWriter cw = new ClassWriter(0);
        cw.visit(Opcodes.V1_8, Opcodes.ACC_PUBLIC, SAMPLE, null, "java/lang/Object", null);

        MethodVisitor constructor = cw.visitMethod(Opcodes.ACC_PUBLIC, "<init>", "()V", null, null);
        constructor.visitCode();
        constructor.visitVarInsn(Opcodes.ALOAD, 0);
        constructor.visitMethodInsn(Opcodes.INVOKESPECIAL, "java/lang/Object", "<init>", "()V", false);
        constructor.visitInsn(Opcodes.RETURN);
        constructor.visitMaxs(1, 1);
        constructor.visitEnd();

        emptyMethod(cw, "(Ljava/lang/String;)V");
        if (withByteBufferOverload)
            emptyMethod(cw, "(Ljava/nio/ByteBuffer;)V");

        cw.visitEnd();
        return cw.toByteArray();
    }

    private static void emptyMethod(ClassWriter cw, String descriptor) {
        MethodVisitor mv = cw.visitMethod(Opcodes.ACC_PUBLIC, "publish", descriptor, null, null);
        mv.visitCode();
        mv.visitInsn(Opcodes.RETURN);
        mv.visitMaxs(0, 2);
        mv.visitEnd();
    }
}
