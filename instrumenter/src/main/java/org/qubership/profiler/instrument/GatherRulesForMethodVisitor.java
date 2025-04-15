package org.qubership.profiler.instrument;

import static org.qubership.profiler.instrument.enhancement.EnhancerConstants.OPCODES_VERSION;

import org.qubership.profiler.configuration.Rule;
import org.qubership.profiler.util.MethodInstrumentationInfo;

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.commons.CodeSizeEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

public class GatherRulesForMethodVisitor extends ClassVisitor {
    private final static Logger log = LoggerFactory.getLogger(GatherRulesForMethodVisitor.class);

    private final Map<String, MethodInstrumentationInfo> selectedRules;
    private final Collection<Rule> rules;
    private final Collection<Rule> methodRules = new ArrayList<Rule>();
    private boolean isInterface = false;
    private String methodName;
    private int methodAccess;
    private int firstLineNumber;
    private int numberOfLines;
    private int numberOfBackJumps;
    HashSet<Label> seenLabels = new HashSet<Label>();

    CodeSizeEvaluator codeSize;

    private final MethodVisitor METHOD_END = new MethodVisitor(OPCODES_VERSION) {
        @Override
        public void visitLineNumber(int line, Label start) {
            if (firstLineNumber == 0)
                firstLineNumber = line;
            numberOfLines++;
        }

        @Override
        public void visitLabel(Label label) {
            seenLabels.add(label);
        }

        @Override
        public void visitJumpInsn(int opcode, Label label) {
            if (seenLabels.remove(label))
                numberOfBackJumps++;
        }

        @Override
        public void visitEnd() {
            final CodeSizeEvaluator sizeEvaluator = GatherRulesForMethodVisitor.this.codeSize;
            final int codeSize = (sizeEvaluator.getMinSize() + sizeEvaluator.getMaxSize()) >> 1;
            if (numberOfLines == 0)
                numberOfLines = (codeSize + 4) / 8;
            for (Rule rule : methodRules) {
                if (rule.matches(methodAccess, methodName, codeSize, numberOfLines, numberOfBackJumps)) {
                    selectedRules.put(methodName, new MethodInstrumentationInfo(rule, firstLineNumber));
                    break;
                }
            }
        }
    };

    public GatherRulesForMethodVisitor(Map<String, MethodInstrumentationInfo> selectedRules, Collection<Rule> rules) {
        super(OPCODES_VERSION);
        this.selectedRules = selectedRules;
        this.rules = rules;
    }

    @Override
    public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
        isInterface = (access & Opcodes.ACC_INTERFACE) > 0;
        methodRules.clear();
        if (isInterface || superName == null) return;

        if ((access & (Opcodes.ACC_PRIVATE | Opcodes.ACC_PUBLIC | Opcodes.ACC_PROTECTED)) == 0)
            access |= Opcodes.ACC_TRANSIENT; // default package-protected visibility

        for (Rule rule : rules)
            if (rule.matches(access, name, superName, interfaces))
                methodRules.add(rule);
    }

    @Override
    public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
        if ((access & Opcodes.ACC_ABSTRACT) > 0 || methodRules.isEmpty()) return null;
        if ((access & (Opcodes.ACC_PRIVATE | Opcodes.ACC_PUBLIC | Opcodes.ACC_PROTECTED)) == 0)
            access |= Opcodes.ACC_TRANSIENT; // default package-protected visibility

        firstLineNumber = 0;
        numberOfLines = 0;
        numberOfBackJumps = 0;
        seenLabels.clear();

        methodAccess = access;
        methodName = name + desc;

        return codeSize = new CodeSizeEvaluator(METHOD_END);
    }


}
