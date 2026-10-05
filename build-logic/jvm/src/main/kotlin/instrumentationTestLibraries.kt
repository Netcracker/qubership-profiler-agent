import org.gradle.api.Project
import org.gradle.api.artifacts.Configuration
import org.gradle.api.file.ConfigurableFileCollection
import org.gradle.api.provider.Property
import org.gradle.api.tasks.Classpath
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.SourceSetContainer
import org.gradle.api.tasks.testing.Test
import org.gradle.kotlin.dsl.getByName
import org.gradle.kotlin.dsl.named
import org.gradle.kotlin.dsl.newInstance
import org.gradle.kotlin.dsl.the
import org.gradle.process.CommandLineArgumentProvider
import java.io.File

/** The versions of one library a plugin instrumentation test runs against. */
data class InstrumentationVersions(val pinned: List<String>, val trackLatest: Boolean)

/**
 * The versions to hold still, and whether to add the one `bom-testing` pins.
 *
 * Leave [trackLatest] on wherever the plugin does instrument the current release: Renovate updates
 * `bom-testing`, so a release that breaks the instrumentation then fails in the Renovate pull
 * request. Turn it off only where the plugin is known not to support the current release, and say in
 * the call why.
 */
fun versions(vararg pinned: String, trackLatest: Boolean = true) =
    InstrumentationVersions(pinned.toList(), trackLatest)

/**
 * Checks this plugin's instrumentation against the real libraries it instruments.
 *
 * Each entry is a Maven coordinate without a version and the versions to check it at, as in
 * `instrumentationTestLibraries("com.rabbitmq:amqp-client" to versions("5.30.0", "5.31.0"))`.
 * Renovate ignores `plugins/<name>/build.gradle.kts`, so a version written here stays where it is
 * put, and the version that moves is the one `bom-testing` carries.
 *
 * The test itself names no class: `PluginInstrumentation` reads this plugin's own configuration and
 * checks every class the configuration selects in each library.
 */
fun Project.instrumentationTestLibraries(vararg libraries: Pair<String, InstrumentationVersions>) {
    dependencies.add("testImplementation", project(":plugin-testkit"))

    val providers = mutableListOf<InstrumentationLibrary>()
    for ((coordinates, versions) in libraries) {
        val artifactId = coordinates.substringAfterLast(':')
        for (version in versions.pinned) {
            providers += libraryProvider(providers.size, artifactId, resolvable(artifactId, version) {
                dependencies.add(it, "$coordinates:$version")
            })
        }
        if (versions.trackLatest) {
            providers += libraryProvider(providers.size, artifactId, resolvable(artifactId, "fromBom") {
                dependencies.add(it, dependencies.platform(project(":bom-testing")))
                dependencies.add(it, coordinates)
            })
        }
    }

    val configDir = File(the<SourceSetContainer>().getByName("main").output.resourcesDir!!, "config")
    tasks.named<Test>("test") {
        systemProperty("profiler.instrumentation.config.dir", configDir.absolutePath)
        systemProperty("profiler.instrumentation.library.count", providers.size)
        jvmArgumentProviders.addAll(providers)
    }
}

private fun Project.resolvable(artifactId: String, suffix: String, declare: (String) -> Unit): Configuration {
    val name = "instrumentation_${artifactId}_$suffix".replace('.', '_').replace('-', '_')
    val declared = configurations.dependencyScope(name) {
        description = "Declares $artifactId $suffix, instrumented by the plugin instrumentation test"
    }
    declare(declared.name)
    return configurations.resolvable("${name}Classpath") { extendsFrom(declared.get()) }.get()
}

private fun Project.libraryProvider(index: Int, artifactId: String, classpath: Configuration) =
    objects.newInstance<InstrumentationLibrary>().apply {
        this.index.set(index)
        this.artifactId.set(artifactId)
        jars.from(classpath)
    }

/**
 * Passes one library distribution to the test.
 *
 * The case name comes from the resolved jar rather than from the declared version, so a version that
 * `bom-testing` moves still names itself in the test report.
 */
abstract class InstrumentationLibrary : CommandLineArgumentProvider {
    @get:Input
    abstract val index: Property<Int>

    @get:Input
    abstract val artifactId: Property<String>

    @get:Classpath
    abstract val jars: ConfigurableFileCollection

    override fun asArguments(): Iterable<String> {
        val prefix = "profiler.instrumentation.library.${index.get()}"
        val id = artifactId.get()
        val jarName = Regex("${Regex.escape(id)}-(\\d[^/]*)\\.jar")
        val name = jars.files.asSequence()
            .mapNotNull { jarName.matchEntire(it.name) }
            .map { "$id ${it.groupValues[1]}" }
            .firstOrNull()
            ?: id
        return listOf("-D$prefix.name=$name", "-D$prefix.classpath=${jars.asPath}")
    }
}
