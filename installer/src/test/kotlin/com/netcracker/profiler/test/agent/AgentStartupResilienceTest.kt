package com.netcracker.profiler.test.agent

import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.startupcheck.OneShotStartupCheckStrategy
import org.testcontainers.images.builder.Transferable
import org.testcontainers.utility.MountableFile
import java.io.ByteArrayOutputStream
import java.time.Duration
import java.util.Objects
import java.util.jar.Attributes
import java.util.jar.JarOutputStream
import java.util.jar.Manifest

/**
 * libinstrument aborts the JVM on any exception leaving `premain`, so a profiler that cannot start
 * itself takes the application down with it. These tests reproduce the two ways `/app/diag/lib`
 * goes wrong in the field, and require the application to run regardless.
 *
 * See [issue #412](https://github.com/Netcracker/qubership-profiler-agent/issues/412): a stale copy
 * of the runtime JAR next to the current one gave each copy its own `PluginClassLoader`, every
 * enhancer then failed to cast to the `EnhancerPlugin` of the other copy, and the service died with
 * `FATAL ERROR in native method: processing of -javaagent failed`.
 */
class AgentStartupResilienceTest {
    companion object {
        private val TESTAPP_JAR: String = Objects.requireNonNull(
            System.getProperty("qubership.profiler.testapp.jar"),
            "system property qubership.profiler.testapp.jar"
        )

        private val CORE_BASE_IMAGE_TAG: String = Objects.requireNonNull(
            System.getProperty("qubership.profiler.java-base-image.tag"),
            "system property qubership.profiler.java-base-image.tag"
        )

        private const val LIB = "/app/diag/lib"

        /** Printed by the test application once it is running under the agent. */
        private const val APPLICATION_STARTED = "hello, world!"
    }

    @Test
    fun `application starts when lib holds a stale copy of the runtime jar`() {
        val container = testApplication("[stale-runtime] ")
            // A rebased image, an init container, or a mount can leave an older runtime JAR behind.
            // Copying the current one reproduces that without pinning the test to a past release.
            .withCommand(
                "sh", "-c",
                "cp $LIB/qubership-profiler-runtime.jar $LIB/stale-runtime.jar && " +
                    "exec java -jar /app/testapp.jar 1"
            )

        val logs = container.use {
            it.start()
            it.logs
        }

        assertApplicationRan(logs, "a duplicated runtime JAR in $LIB")
        assertTrue(logs.contains("is provided by several JARs")) {
            "Expected the duplicated runtime JAR to be reported.\n\n$logs"
        }
        // Dropping every copy would leave the service unprofiled, which is what the duplicate
        // warning is meant to prevent -- the newest copy has to stay.
        assertTrue(logs.contains("Profiler: initialized, version")) {
            "Expected the profiler to keep working on the surviving runtime JAR.\n\n$logs"
        }
    }

    @Test
    fun `application starts when a plugin jar cannot be loaded`() {
        val container = testApplication("[broken-plugin] ")
            .withCopyToContainer(Transferable.of(brokenPluginJar()), "$LIB/broken-plugin.jar")
            .withCommand("java", "-jar", "/app/testapp.jar", "1")

        val logs = container.use {
            it.start()
            it.logs
        }

        assertApplicationRan(logs, "an unloadable plugin JAR in $LIB")
        // This also keeps the scenario honest: a plugin dropped unread would leave the agent
        // healthy, and the test would stop exercising the premain boundary it was written for.
        assertTrue(logs.contains("the application continues without profiling")) {
            "Expected the agent to load the broken plugin and then give up on profiling.\n\n$logs"
        }
    }

    @Test
    fun `profiling survives an unreadable file in lib`() {
        val container = testApplication("[corrupt-jar] ")
            // A half-finished copy or a truncated download lands here as a file the JAR reader
            // cannot open at all, which is a different case from a JAR that merely fails to load.
            .withCopyToContainer(
                Transferable.of("this is not a JAR"),
                "$LIB/qubership-profiler-plugins-truncated.jar"
            )
            .withCommand("java", "-jar", "/app/testapp.jar", "1")

        val logs = container.use {
            it.start()
            it.logs
        }

        assertApplicationRan(logs, "an unreadable file in $LIB")
        // One stray file must cost the plugin it is, not the whole agent.
        assertTrue(logs.contains("Profiler: initialized, version")) {
            "Expected the profiler to keep loading around the unreadable file.\n\n$logs"
        }
    }

    private fun assertApplicationRan(logs: String, scenario: String) {
        assertTrue(logs.contains(APPLICATION_STARTED)) {
            "The application did not start with $scenario.\n\n$logs"
        }
        // libinstrument aborts the JVM on any exception leaving premain, and the entrypoint keeps
        // going afterwards, so the abort has to be caught in the log rather than in the exit code.
        assertTrue(!logs.contains("processing of -javaagent failed")) {
            "The agent aborted the JVM with $scenario.\n\n$logs"
        }
    }

    /**
     * A one-shot run of the test application under the base image, so that `start()` fails unless
     * the JVM ran the application to completion and exited with 0.
     */
    private fun testApplication(logPrefix: String): GenericContainer<*> =
        GenericContainer(CORE_BASE_IMAGE_TAG)
            .withEnv("ESC_LOG_LEVEL", "debug")
            .withEnv("PROFILER_ENABLED", "true")
            .withEnv("NC_DIAGNOSTIC_MODE", "prod")
            .withEnv("CLOUD_NAMESPACE", "test-namespace")
            .withEnv("MICROSERVICE_NAME", "test-app")
            .withCopyToContainer(MountableFile.forHostPath(TESTAPP_JAR), "/app/testapp.jar")
            .withStartupAttempts(1)
            .withStartupTimeout(Duration.ofMinutes(2))
            .withLogConsumer(LogToConsolePrinter(logPrefix))
            .withStartupCheckStrategy(OneShotStartupCheckStrategy())

    /**
     * A manifest-only JAR whose single entry point does not exist, which is the cheapest way to make
     * plugin loading throw.
     */
    private fun brokenPluginJar(): ByteArray {
        val manifest = Manifest().apply {
            mainAttributes[Attributes.Name.MANIFEST_VERSION] = "1.0"
            mainAttributes.putValue("Entry-Points", "com.example.MissingPlugin")
            mainAttributes.putValue("Implementation-Version", "9.9.9")
        }
        val jar = ByteArrayOutputStream()
        JarOutputStream(jar, manifest).close()
        return jar.toByteArray()
    }
}
