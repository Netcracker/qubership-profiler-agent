package com.netcracker.profiler.agent;

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.tree.FrameNode;
import org.objectweb.asm.tree.JumpInsnNode;
import org.objectweb.asm.tree.LabelNode;
import org.objectweb.asm.tree.LineNumberNode;
import org.objectweb.asm.tree.LocalVariableNode;
import org.objectweb.asm.tree.MethodNode;
import org.objectweb.asm.tree.TryCatchBlockNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StaticInitMerger extends ClassVisitor {
    private final List<MethodNode> clinitNodes = new ArrayList<>();
    private String clinitDescriptor;

    // saved from visit(...) to allow temporary ClassWriter computing frames for merged method
    private int classVersion = Opcodes.V1_8;
    private int classAccess = Opcodes.ACC_PUBLIC;
    private String ownerInternalName;
    private String superName;
    private String[] interfaces;

    public StaticInitMerger(final ClassVisitor classVisitor) {
        this(Opcodes.ASM9, classVisitor);
    }

    protected StaticInitMerger(final int api, final ClassVisitor classVisitor) {
        super(api, classVisitor);
    }

    @Override
    public void visit(
            final int version,
            final int access,
            final String name,
            final String signature,
            final String superName,
            final String[] interfaces) {
        super.visit(version, access, name, signature, superName, interfaces);
        this.classVersion = version;
        this.classAccess = access;
        this.ownerInternalName = name;
        this.superName = superName;
        this.interfaces = interfaces == null ? null : interfaces.clone();
    }

    @Override
    public MethodVisitor visitMethod(
            final int access,
            final String name,
            final String descriptor,
            final String signature,
            final String[] exceptions) {
        if ("<clinit>".equals(name)) {
            if (clinitDescriptor == null) {
                clinitDescriptor = descriptor;
            }

            MethodNode mn = new MethodNode(api, access, name, descriptor, signature, exceptions);
            clinitNodes.add(mn);
            return mn;
        }
        return super.visitMethod(access, name, descriptor, signature, exceptions);
    }

    @Override
    public void visitEnd() {
        if (!clinitNodes.isEmpty()) {
            if (!"()V".equals(clinitDescriptor)) {
                throw new IllegalStateException("Unsupported <clinit> descriptor: " + clinitDescriptor);
            }

            MethodNode merged = new MethodNode(api, Opcodes.ACC_STATIC, "<clinit>", clinitDescriptor, null, null);

            for (int i = 0; i < clinitNodes.size(); i++) {
                MethodNode mn = clinitNodes.get(i);

                // Map old labels to new labels when cloning instructions
                Map<LabelNode, LabelNode> labelMap = new HashMap<>();

                // First pass: pre-create mappings for all LabelNode and LineNumberNode starts
                for (AbstractInsnNode insn = mn.instructions.getFirst(); insn != null; insn = insn.getNext()) {
                    if (insn instanceof LabelNode) {
                        LabelNode ln = (LabelNode) insn;
                        if (!labelMap.containsKey(ln)) {
                            labelMap.put(ln, new LabelNode());
                        }
                    } else if (insn instanceof LineNumberNode) {
                        LineNumberNode lnn = (LineNumberNode) insn;
                        LabelNode start = lnn.start;
                        if (start != null && !labelMap.containsKey(start)) {
                            labelMap.put(start, new LabelNode());
                        }
                    }
                }

                // Clone instructions but skip FrameNode instances
                List<AbstractInsnNode> toAdd = new ArrayList<>();
                for (AbstractInsnNode insn = mn.instructions.getFirst(); insn != null; insn = insn.getNext()) {
                    if (insn instanceof FrameNode) {
                        continue;
                    }
                    AbstractInsnNode clone = insn.clone(labelMap);
                    if (clone != null) {
                        toAdd.add(clone);
                    }
                }

                // Replace RETURN -> GOTO(boundary) for all but last method
                LabelNode boundaryLabel = null;
                if (i != clinitNodes.size() - 1) {
                    boundaryLabel = new LabelNode();
                }

                for (AbstractInsnNode ain : toAdd) {
                    if (ain == null) continue;
                    int opcode = ain.getOpcode();
                    if (opcode == Opcodes.RETURN && boundaryLabel != null) {
                        merged.instructions.add(new JumpInsnNode(Opcodes.GOTO, boundaryLabel));
                    } else {
                        merged.instructions.add(ain);
                    }
                }

                if (boundaryLabel != null) {
                    merged.instructions.add(boundaryLabel);
                }

                // Clone try/catch blocks with remapped labels
                if (mn.tryCatchBlocks != null) {
                    for (TryCatchBlockNode tcb : mn.tryCatchBlocks) {
                        LabelNode start = remapLabel(tcb.start, labelMap);
                        LabelNode end = remapLabel(tcb.end, labelMap);
                        LabelNode handler = remapLabel(tcb.handler, labelMap);
                        merged.tryCatchBlocks.add(new TryCatchBlockNode(start, end, handler, tcb.type));
                    }
                }

                // Clone local variables with remapped labels
                if (mn.localVariables != null) {
                    for (LocalVariableNode lv : mn.localVariables) {
                        LabelNode start = remapLabel(lv.start, labelMap);
                        LabelNode end = remapLabel(lv.end, labelMap);
                        merged.localVariables.add(new LocalVariableNode(lv.name, lv.desc, lv.signature, start, end, lv.index));
                    }
                }
            }

            // Compute frames for merged method using temporary ClassWriter with COMPUTE_FRAMES
            MethodNode computed = computeFramesWithClassWriter(merged);
            if (computed == null) {
                // fallback
                computed = merged;
            }

            // Emit computed merged <clinit>
            MethodVisitor mv = super.visitMethod(Opcodes.ACC_STATIC, "<clinit>", clinitDescriptor, null, null);
            computed.accept(mv);

            mv.visitEnd();
        }
        super.visitEnd();
    }

    private MethodNode computeFramesWithClassWriter(MethodNode merged) {
        try {
            // Build minimal ClassNode containing only merged method
            ClassNode tmp = new ClassNode();
            tmp.version = this.classVersion;
            tmp.access = this.classAccess;
            tmp.name = this.ownerInternalName;
            tmp.superName = this.superName == null ? "java/lang/Object" : this.superName;
            if (this.interfaces != null) {
                tmp.interfaces.addAll(Arrays.asList(this.interfaces));
            }

            MethodNode tmpMethod = new MethodNode(merged.access, merged.name, merged.desc, merged.signature, null);
            merged.accept(tmpMethod);
            tmp.methods = new ArrayList<>();
            tmp.methods.add(tmpMethod);

            ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS);
            tmp.accept(cw);
            byte[] bytes = cw.toByteArray();

            ClassNode out = new ClassNode();
            ClassReader cr = new ClassReader(bytes);
            cr.accept(out, 0);

            for (MethodNode mn : out.methods) {
                if (mn.name.equals(tmpMethod.name) && mn.desc.equals(tmpMethod.desc)) {
                    return mn;
                }
            }
        } catch (Throwable t) {
            // ignore and fall through to fallback
        }
        return null;
    }

    private static LabelNode remapLabel(LabelNode original, Map<LabelNode, LabelNode> map) {
        if (original == null) return null;
        return map.computeIfAbsent(original, k -> new LabelNode());
    }
}
