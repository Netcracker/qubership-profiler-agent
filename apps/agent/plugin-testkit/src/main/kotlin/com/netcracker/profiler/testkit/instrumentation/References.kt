package com.netcracker.profiler.testkit.instrumentation

import org.objectweb.asm.ClassReader
import org.objectweb.asm.Handle
import org.objectweb.asm.Opcodes
import org.objectweb.asm.Type
import org.objectweb.asm.tree.ClassNode
import org.objectweb.asm.tree.FieldInsnNode
import org.objectweb.asm.tree.InvokeDynamicInsnNode
import org.objectweb.asm.tree.MethodInsnNode
import org.objectweb.asm.tree.TypeInsnNode

import java.lang.reflect.Modifier

enum class ReferenceKind { METHOD, FIELD, TYPE }

/** A member or type one class file names, as the constant pool spells it. */
data class Reference(
    val kind: ReferenceKind,
    val owner: String,
    val name: String = "",
    val descriptor: String = "",
    val static: Boolean = false,
) {
    constructor(handle: Handle) : this(
        if (handle.tag >= Opcodes.H_INVOKEVIRTUAL) ReferenceKind.METHOD else ReferenceKind.FIELD,
        handle.owner,
        handle.name,
        handle.desc,
        static = handle.tag == Opcodes.H_INVOKESTATIC || handle.tag == Opcodes.H_GETSTATIC ||
            handle.tag == Opcodes.H_PUTSTATIC,
    )

    override fun toString(): String = when (kind) {
        ReferenceKind.METHOD -> "$owner.$name$descriptor"
        ReferenceKind.FIELD -> "$owner.$name:$descriptor"
        ReferenceKind.TYPE -> owner
    }
}

internal fun classNode(classFile: ByteArray): ClassNode =
    ClassNode().also { ClassReader(classFile).accept(it, ClassReader.SKIP_FRAMES) }

/**
 * Collects the method, field and type instructions of every body in [classFile], the handles an
 * `invokedynamic` names, and the handler type of every `try`/`catch`.
 *
 * The bootstrap arguments matter because the profiler reaches an `execute-after` target through
 * `invokedynamic` where `ADD_INDY_TRY_CATCH_BLOCKS` is on, which is the default: the target then
 * appears nowhere else in the class file. The handler types matter because `ProfileMethodAdapter`
 * adds a `java/lang/Throwable` handler to every instrumented method other than a constructor.
 *
 * Left out: the types a member's own descriptor and signature name, which a call site names again
 * where it matters; a class or method-type constant loaded with `LDC`; the element type of a
 * `MULTIANEWARRAY`; and the bootstrap arguments of a `ConstantDynamic`.
 */
fun references(classFile: ByteArray): Set<Reference> {
    val references = LinkedHashSet<Reference>()
    for (method in classNode(classFile).methods) {
        for (instruction in method.instructions) {
            when (instruction) {
                is MethodInsnNode -> references += Reference(
                    ReferenceKind.METHOD, instruction.owner, instruction.name, instruction.desc,
                    static = instruction.opcode == Opcodes.INVOKESTATIC
                )
                is FieldInsnNode -> references += Reference(
                    ReferenceKind.FIELD, instruction.owner, instruction.name, instruction.desc,
                    static = instruction.opcode == Opcodes.GETSTATIC || instruction.opcode == Opcodes.PUTSTATIC
                )
                is TypeInsnNode -> references += Reference(ReferenceKind.TYPE, instruction.desc)
                is InvokeDynamicInsnNode -> {
                    references += Reference(instruction.bsm)
                    instruction.bsmArgs.filterIsInstance<Handle>().forEach { references += Reference(it) }
                }
            }
        }
        for (handler in method.tryCatchBlocks) {
            handler.type?.let { references += Reference(ReferenceKind.TYPE, it) }
        }
    }
    return references
}

/**
 * Returns why [reference] cannot be resolved against [loader], or null when it resolves.
 *
 * A member the enhancer injected is declared by [instrumented] and by nothing on [loader], so the
 * transformed class is searched before the loaded one. Declaration and staticness are modelled;
 * access control is not. An injected body does name members outside the enhanced class, but those
 * are public, and a member the injector declares is reached by the search of [instrumented] whatever
 * its access, so a package-private or protected target does not arise here.
 *
 * This is a linkage check, not a verification one: the JVM resolves a member when the instruction
 * that names it first runs, so neither the verifier nor `CheckClassAdapter` reports a target that
 * does not exist.
 */
fun unresolved(reference: Reference, instrumented: ClassNode, loader: ClassLoader): String? {
    if (reference.kind == ReferenceKind.TYPE) {
        val element = when {
            reference.owner.startsWith("[") -> Type.getType(reference.owner).elementType
            else -> Type.getObjectType(reference.owner)
        }
        if (element.sort != Type.OBJECT || loads(element.internalName, loader) != null) {
            return null
        }
        return "$reference (class not found)"
    }
    if (reference.owner.startsWith("[")) {
        // An array declares no member of its own; the reference is to one java.lang.Object declares.
        return null
    }
    if (reference.owner == instrumented.name) {
        val declared = declaredBy(instrumented, reference)
        if (declared != Match.ABSENT) {
            return reason(declared, reference)
        }
    }
    val owner = loads(reference.owner, loader) ?: return "$reference (class not found)"
    return reason(search(owner, reference), reference)
}

private enum class Match { FOUND, STATIC_MISMATCH, ABSENT }

private fun reason(match: Match, reference: Reference): String? = when (match) {
    Match.FOUND -> null
    Match.STATIC_MISMATCH -> "$reference (static mismatch)"
    Match.ABSENT -> "$reference (no such member)"
}

private fun loads(internalName: String, loader: ClassLoader): Class<*>? =
    try {
        Class.forName(internalName.replace('/', '.'), false, loader)
    } catch (notFound: ClassNotFoundException) {
        null
    } catch (broken: LinkageError) {
        null
    }

private fun declaredBy(node: ClassNode, reference: Reference): Match {
    val access: List<Int> = when (reference.kind) {
        ReferenceKind.METHOD -> node.methods
            .filter { it.name == reference.name && it.desc == reference.descriptor }
            .map { it.access }
        ReferenceKind.FIELD -> node.fields
            .filter { it.name == reference.name && it.desc == reference.descriptor }
            .map { it.access }
        ReferenceKind.TYPE -> emptyList()
    }
    if (access.isEmpty()) {
        return Match.ABSENT
    }
    return match(access.map { (it and Opcodes.ACC_STATIC) != 0 }, reference)
}

private fun search(owner: Class<*>, reference: Reference): Match {
    if (reference.name == "<init>") {
        val found = owner.declaredConstructors.filter { Type.getConstructorDescriptor(it) == reference.descriptor }
        return if (found.isEmpty()) Match.ABSENT else Match.FOUND
    }
    val pending = ArrayDeque<Class<*>>()
    pending.add(owner)
    val seen = HashSet<Class<*>>()
    var result = Match.ABSENT
    while (pending.isNotEmpty()) {
        val type = pending.removeFirst()
        if (!seen.add(type)) {
            continue
        }
        val declared = when (reference.kind) {
            ReferenceKind.METHOD -> type.declaredMethods
                .filter { it.name == reference.name && Type.getMethodDescriptor(it) == reference.descriptor }
                .map { it.modifiers }
            ReferenceKind.FIELD -> type.declaredFields
                .filter { it.name == reference.name && Type.getDescriptor(it.type) == reference.descriptor }
                .map { it.modifiers }
            ReferenceKind.TYPE -> emptyList()
        }
        // A private member is not inherited, so one declared above the owner is not this reference's.
        val modifiers = declared.filter { type == owner || !Modifier.isPrivate(it) }
        if (modifiers.isNotEmpty()) {
            val verdict = match(modifiers.map { Modifier.isStatic(it) }, reference)
            if (verdict == Match.FOUND) {
                return Match.FOUND
            }
            result = verdict
        }
        type.superclass?.let { pending.add(it) }
        pending.addAll(type.interfaces)
    }
    return result
}

private fun match(staticness: List<Boolean>, reference: Reference): Match =
    if (staticness.any { it == reference.static }) Match.FOUND else Match.STATIC_MISMATCH
