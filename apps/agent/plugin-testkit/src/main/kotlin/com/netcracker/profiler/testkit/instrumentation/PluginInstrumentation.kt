package com.netcracker.profiler.testkit.instrumentation

import com.netcracker.profiler.agent.ProfilingTransformer
import com.netcracker.profiler.agent.plugins.EnhancerRegistryPluginImpl
import com.netcracker.profiler.configuration.ConfigurationImpl
import com.netcracker.profiler.configuration.Rule
import com.netcracker.profiler.instrument.GatherRulesForMethodVisitor
import com.netcracker.profiler.instrument.enhancement.ClassInfo
import com.netcracker.profiler.instrument.enhancement.ClassInfoImpl
import com.netcracker.profiler.instrument.enhancement.EnhancerPlugin
import com.netcracker.profiler.util.MethodInstrumentationInfo

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.params.provider.Arguments
import org.objectweb.asm.ClassReader
import org.objectweb.asm.tree.MethodInsnNode
import org.objectweb.asm.util.CheckClassAdapter
import org.w3c.dom.Element
import org.w3c.dom.NodeList

import java.io.ByteArrayOutputStream
import java.io.File
import java.io.PrintWriter
import java.net.URLClassLoader
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.security.CodeSource
import java.security.ProtectionDomain
import java.security.cert.Certificate
import java.util.jar.JarFile
import java.util.stream.Collectors
import javax.xml.parsers.DocumentBuilderFactory

/** One distribution of a third-party library the plugin under test is checked against. */
data class LibraryUnderTest(val name: String, val classpath: List<Path>) {
    override fun toString(): String = name
}

/**
 * Checks a profiler plugin against the real libraries it instruments.
 *
 * A plugin binds an injected `$profiler` method by the exact descriptor of the call site, and
 * `ExecuteMethod` builds that descriptor from the parameter types of the method the rule matched. A
 * library that adds an overload therefore produces a call to a method the injector may not declare,
 * and the JVM only reports it, as `NoSuchMethodError`, when the instrumented method first runs.
 * These checks read the transformed bytecode instead, so a missing member fails the build.
 *
 * Nothing here names a class: the harness reads the plugin's own configuration, asks
 * [ProfilingTransformer.transformRequired] about every class in the library, and checks the ones it
 * selects. A plugin's test therefore lists the libraries and their versions and nothing else.
 *
 * Gradle passes the libraries in the `profiler.instrumentation.*` system properties; see
 * `instrumentationTestLibraries` in `build-logic`.
 */
object PluginInstrumentation {
    private const val CONFIG_DIR = "profiler.instrumentation.config.dir"
    private const val LIBRARY_COUNT = "profiler.instrumentation.library.count"

    /** The distributions Gradle resolved for this module, one parameterized case each. */
    @JvmStatic
    fun librariesUnderTest(): List<Arguments> {
        val count = System.getProperty(LIBRARY_COUNT)
            ?: error("$LIBRARY_COUNT is not set; run this test through Gradle")
        return (0 until count.toInt()).map { index ->
            val name = System.getProperty("profiler.instrumentation.library.$index.name")
            val classpath = System.getProperty("profiler.instrumentation.library.$index.classpath")
                .split(File.pathSeparator)
                .map { Paths.get(it) }
            Arguments.argumentSet(name, LibraryUnderTest(name, classpath))
        }
    }

    /**
     * Fails when a member or type the transformation added to a class of [library] is not declared.
     *
     * This is the check that catches an injector with no method for an overload the library added.
     */
    @JvmStatic
    fun assertAddedReferencesResolve(library: LibraryUnderTest) {
        withInstrumented(library) { loader, instrumented ->
            val unresolved = instrumented.flatMap { subject ->
                val node = classNode(subject.transformed)
                (references(subject.transformed) - references(subject.original))
                    .mapNotNull { unresolved(it, node, loader) }
            }
            assertEquals(emptyList<String>(), unresolved, "references the profiler added on $library")
        }
    }

    /** Fails when a class the transformation rewrote no longer passes bytecode verification. */
    @JvmStatic
    fun assertInstrumentedClassesVerify(library: LibraryUnderTest) {
        withInstrumented(library) { loader, instrumented ->
            val broken = instrumented.mapNotNull { subject ->
                val report = ByteArrayOutputStream()
                PrintWriter(report, true).use {
                    CheckClassAdapter.verify(ClassReader(subject.transformed), loader, false, it)
                }
                report.toString("UTF-8").takeIf { it.isNotEmpty() }?.let { "${subject.internalName}: $it" }
            }
            assertEquals(emptyList<String>(), broken, "CheckClassAdapter.verify on $library")
        }
    }

    /**
     * Fails when the configuration selects a class of [library] but no method of it is rewritten,
     * when a method it selects keeps the body it had, or when the library comes through with no
     * method instrumented at all.
     *
     * Without this the other checks pass on a plugin that instruments nothing: a rule that stops
     * matching adds no reference to resolve and rewrites no body to verify. The last of the three is
     * what makes the check unfalsifiable otherwise, since [withInstrumented] only requires a class
     * the configuration named: `transform` hands back a rewritten copy of a class whose rules a
     * later filter dropped, so a plugin whose rules all fall away still produces subjects.
     */
    @JvmStatic
    fun assertSelectedMethodsAreInstrumented(library: LibraryUnderTest) {
        withInstrumented(library) { _, instrumented ->
            val missed = instrumented.flatMap { subject ->
                val before = bodySizes(subject.original)
                val after = bodySizes(subject.transformed)
                when {
                    subject.rules.isEmpty() -> emptyList()
                    subject.selectedMethods.isEmpty() ->
                        listOf("${subject.internalName}: rules match the class, no method")
                    else -> subject.selectedMethods.keys
                        .filter { before[it] == after[it] }
                        .map { "${subject.internalName}.$it: selected, body unchanged" }
                }
            }
            assertEquals(emptyList<String>(), missed, "methods the configuration selects on $library")
            assertNotEquals(
                emptyList<String>(),
                instrumented.filter { it.selectedMethods.isNotEmpty() }.map { it.internalName },
                "classes of $library with at least one method the configuration selects"
            )
        }
    }

    /**
     * Fails when a method the configuration selects calls another method it selects on the same
     * class, which runs two instrumented frames for one operation.
     *
     * A library that turns an overload into a delegation to a wider one leaves both under the
     * configuration, and the caller then pays two enter and exit pairs in the call tree and two
     * copies of every event the injected methods record.
     *
     * This one is opt-in, and [PluginInstrumentationTest] does not run it, because nesting is
     * sometimes the point: `apache_httpclient` profiles `HttpRequestExecutor.execute` and the
     * `doSendRequest` and `doReceiveResponse` it calls, to show where the time inside the call goes.
     * Call it from a plugin whose rules are meant to claim one method per operation. Only a direct
     * call is reported, so a delegation through a method no rule selects still gets past it.
     */
    @JvmStatic
    fun assertNoOperationIsInstrumentedTwice(library: LibraryUnderTest) {
        withInstrumented(library) { _, instrumented ->
            val nested = instrumented.flatMap { subject ->
                val original = classNode(subject.original)
                subject.selectedMethods.keys.flatMap { selected ->
                    val body = original.methods.first { it.name + it.desc == selected }
                    body.instructions.asSequence()
                        .filterIsInstance<MethodInsnNode>()
                        .filter { it.owner == subject.internalName && it.name + it.desc in subject.selectedMethods }
                        .map { "${subject.internalName}.$selected calls ${it.name}${it.desc}" }
                        .toList()
                }
            }
            assertEquals(emptyList<String>(), nested, "methods the configuration selects that call one another on $library")
        }
    }

    private class Subject(
        val internalName: String,
        val original: ByteArray,
        val transformed: ByteArray,
        val rules: Collection<Rule>,
        val selectedMethods: Map<String, Rule>,
    )

    private fun <T> withInstrumented(library: LibraryUnderTest, body: (ClassLoader, List<Subject>) -> T): T =
        URLClassLoader(
            library.classpath.map { it.toUri().toURL() }.toTypedArray(),
            PluginInstrumentation::class.java.classLoader
        ).use { loader ->
            val subjects = instrument(library)
            assertNotEquals(
                emptyList<String>(),
                subjects.map { it.internalName },
                "classes of $library the configuration in ${configDir()} selects"
            )
            body(loader, subjects)
        }

    private fun instrument(library: LibraryUnderTest): List<Subject> =
        library.classpath.filter { it.fileName.toString().endsWith(".jar") }.flatMap { jar ->
            val domain = ProtectionDomain(CodeSource(jar.toUri().toURL(), null as Array<Certificate>?), null)
            JarFile(jar.toFile()).use { file ->
                file.entries().asSequence()
                    .filter { it.name.endsWith(".class") }
                    .flatMap { entry ->
                        val internalName = entry.name.removeSuffix(".class")
                        val original by lazy { file.getInputStream(entry).use { it.readBytes() } }
                        transformers.mapNotNull { transformer ->
                            if (!transformer.transformRequired(internalName)) {
                                return@mapNotNull null
                            }
                            val transformed = transformer.transform(null, internalName, null, domain, original)
                                ?: return@mapNotNull null
                            val rules = rulesFor(transformer, internalName, domain, original)
                            Subject(internalName, original, transformed, rules, selectedMethods(original, rules))
                        }
                    }
                    .toList()
            }
        }

    /**
     * The rules the transformer would apply to [internalName], with the `if-enhancer` and the
     * class-structure filters already applied, exactly as `ProfilingTransformer.transform` applies
     * them.
     */
    private fun rulesFor(
        transformer: ProfilingTransformer,
        internalName: String,
        domain: ProtectionDomain,
        original: ByteArray,
    ): Collection<Rule> {
        val configuration = transformer.configuration
        val registry = configuration.enhancementRegistry
        val info: ClassInfo = ClassInfoImpl().apply {
            className = internalName
            protectionDomain = domain
        }
        val declaredMethods by lazy { classNode(original).methods.map { it.name + it.desc } }
        return configuration.getRulesForClass(internalName, null).filter { rule ->
            val ifEnhancer = rule.ifEnhancer
            if (ifEnhancer != null && (registry.getFilter(ifEnhancer) as EnhancerPlugin?)?.accept(info) == false) {
                return@filter false
            }
            !rule.hasClassStructureCriteria() || rule.matchesClassStructure(declaredMethods)
        }
    }

    /**
     * The methods [rules] select in [original], keyed the way the profiler keys them, name plus
     * descriptor, and mapped to the rule that claimed each one.
     *
     * `GatherRulesForMethodVisitor` stops at the first rule that matches a method, so the mapping is
     * the one the transformer itself will use.
     */
    private fun selectedMethods(original: ByteArray, rules: Collection<Rule>): Map<String, Rule> {
        if (rules.isEmpty()) {
            return emptyMap()
        }
        val selected = HashMap<String, MethodInstrumentationInfo>()
        ClassReader(original).accept(GatherRulesForMethodVisitor(selected, rules), ClassReader.SKIP_FRAMES)
        return selected.mapValues { it.value.rule }
    }

    private fun bodySizes(classFile: ByteArray): Map<String, Int> =
        classNode(classFile).methods.associate { it.name + it.desc to it.instructions.size() }

    private fun configDir(): Path =
        Paths.get(System.getProperty(CONFIG_DIR) ?: error("$CONFIG_DIR is not set; run this test through Gradle"))

    /** One transformer per configuration file the plugin ships, with its enhancers registered. */
    private val transformers: List<ProfilingTransformer> by lazy {
        EnhancerRegistryPluginImpl()
        val configs = Files.walk(configDir()).use { paths ->
            paths.filter { Files.isRegularFile(it) && it.fileName.toString().endsWith(".xml") }
                .filter { it.parent.fileName.toString() == "50main" }
                .sorted()
                .collect(Collectors.toList())
        }
        check(configs.isNotEmpty()) { "no */50main/*.xml under ${configDir()}" }
        configs.map { config ->
            registerEnhancers(config)
            ProfilingTransformer(ConfigurationImpl(config.toString()))
        }
    }

    /** Instantiates `EnhancerPlugin_<name>` for every `<enhancer>` the configuration declares. */
    private fun registerEnhancers(config: Path) {
        val document = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(config.toFile())
        val declared: NodeList = document.getElementsByTagName("enhancer")
        for (index in 0 until declared.length) {
            val name = (declared.item(index) as Element).textContent.trim()
            val className = "com.netcracker.profiler.instrument.enhancement.EnhancerPlugin_$name"
            Class.forName(className).getDeclaredConstructor().newInstance()
        }
    }
}
