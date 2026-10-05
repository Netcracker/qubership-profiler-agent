package com.netcracker.profiler.testkit.instrumentation

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.objectweb.asm.Opcodes
import org.objectweb.asm.tree.ClassNode
import org.objectweb.asm.tree.FieldNode
import org.objectweb.asm.tree.MethodNode

/**
 * Negative control for [unresolved], which every plugin instrumentation test rests on: a resolver
 * that reported each input as resolved, or that gave every failure the same reason, would leave the
 * per-plugin checks green over an injector that cannot link.
 */
class ReferencesTest {
    @ParameterizedTest
    @MethodSource("fabricatedReferences")
    fun `a reference is reported with the reason it cannot be resolved`(reference: Reference, reason: String?) {
        assertEquals(reason, unresolved(reference, ClassNode(), javaClass.classLoader), "unresolved($reference)")
    }

    /**
     * The branch every plugin check rests on: a member the enhancer injected is declared by the
     * transformed class and by nothing the class loader can reach, so it has to resolve against the
     * bytes rather than against the loaded class.
     */
    @ParameterizedTest
    @MethodSource("referencesToTheInstrumentedClass")
    fun `a member the enhancer injected resolves against the transformed class`(
        reference: Reference,
        reason: String?,
    ) {
        assertEquals(reason, unresolved(reference, injectedInto(), javaClass.classLoader), "unresolved($reference)")
    }

    companion object {
        /**
         * References [unresolved] has to tell apart, with the line each one owes the failure report.
         *
         * `Profiler.event(Object, String)` is the static method every injected body calls, and the
         * profiler's own runtime is on this module's classpath, so it stands for a class that loads.
         */
        @JvmStatic
        fun fabricatedReferences(): List<Arguments> {
            val profiler = "com/netcracker/profiler/agent/Profiler"
            val event = "(Ljava/lang/Object;Ljava/lang/String;)V"
            return listOf(
                Arguments.argumentSet(
                    "class not found",
                    Reference(ReferenceKind.METHOD, "com/does/not/Exist", "publish", "()V", static = false),
                    "com/does/not/Exist.publish()V (class not found)"
                ),
                Arguments.argumentSet(
                    "no such member",
                    Reference(ReferenceKind.METHOD, profiler, "publish", "()V", static = true),
                    "$profiler.publish()V (no such member)"
                ),
                Arguments.argumentSet(
                    "static mismatch",
                    Reference(ReferenceKind.METHOD, profiler, "event", event, static = false),
                    "$profiler.event$event (static mismatch)"
                ),
                Arguments.argumentSet(
                    "resolved",
                    Reference(ReferenceKind.METHOD, profiler, "event", event, static = true),
                    null
                ),
                Arguments.argumentSet(
                    "a field that resolves",
                    Reference(ReferenceKind.FIELD, "java/lang/Integer", "MAX_VALUE", "I", static = true),
                    null
                ),
                Arguments.argumentSet(
                    "a field the class does not declare",
                    Reference(ReferenceKind.FIELD, "java/lang/Integer", "MAX_VALUES", "I", static = true),
                    "java/lang/Integer.MAX_VALUES:I (no such member)"
                ),
                Arguments.argumentSet(
                    "an instance read of a static field",
                    Reference(ReferenceKind.FIELD, "java/lang/Integer", "MAX_VALUE", "I", static = false),
                    "java/lang/Integer.MAX_VALUE:I (static mismatch)"
                ),
                Arguments.argumentSet(
                    "a type that loads",
                    Reference(ReferenceKind.TYPE, "java/lang/Integer"),
                    null
                ),
                Arguments.argumentSet(
                    "a type that does not load",
                    Reference(ReferenceKind.TYPE, "com/does/not/Exist"),
                    "com/does/not/Exist (class not found)"
                ),
                Arguments.argumentSet(
                    "an array of a primitive, which loads nothing",
                    Reference(ReferenceKind.TYPE, "[I"),
                    null
                ),
                Arguments.argumentSet(
                    "a method on an array, which java.lang.Object declares",
                    Reference(ReferenceKind.METHOD, "[Ljava/lang/String;", "clone", "()Ljava/lang/Object;"),
                    null
                )
            )
        }

        private const val SAMPLE = "com/example/Sample"

        /** A transformed class carrying the members an injector would have added to it. */
        @JvmStatic
        fun injectedInto(): ClassNode = ClassNode().apply {
            name = SAMPLE
            methods.add(MethodNode(Opcodes.ACC_PUBLIC, "publish\$profiler", "(Ljava/lang/String;)V", null, null))
            fields.add(FieldNode(Opcodes.ACC_STATIC, "counter\$profiler", "I", null, null))
        }

        @JvmStatic
        fun referencesToTheInstrumentedClass(): List<Arguments> = listOf(
            Arguments.argumentSet(
                "an injected method",
                Reference(ReferenceKind.METHOD, SAMPLE, "publish\$profiler", "(Ljava/lang/String;)V"),
                null
            ),
            Arguments.argumentSet(
                "an injected field",
                Reference(ReferenceKind.FIELD, SAMPLE, "counter\$profiler", "I", static = true),
                null
            ),
            Arguments.argumentSet(
                "a static call to an injected instance method",
                Reference(ReferenceKind.METHOD, SAMPLE, "publish\$profiler", "(Ljava/lang/String;)V", static = true),
                "$SAMPLE.publish\$profiler(Ljava/lang/String;)V (static mismatch)"
            ),
            Arguments.argumentSet(
                "a member the injector did not declare, which falls through to the class loader",
                Reference(ReferenceKind.METHOD, SAMPLE, "consume\$profiler", "()V"),
                "$SAMPLE.consume\$profiler()V (class not found)"
            )
        )
    }
}
