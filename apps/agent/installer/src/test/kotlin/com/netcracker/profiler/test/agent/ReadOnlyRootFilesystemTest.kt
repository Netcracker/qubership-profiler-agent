package com.netcracker.profiler.test.agent

import org.junit.jupiter.api.Assertions.assertAll
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.testcontainers.containers.BindMode
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.startupcheck.OneShotStartupCheckStrategy
import java.time.Duration
import java.util.Objects

/**
 * With a read-only root filesystem, the agent installed in `/app/diag` writes its runtime data to
 * `/app/ncdiag`, the writable folder the base image documents for that case.
 *
 * The tmpfs mounts are the ones qubership-core-base-images lists for read-only mode. A Kubernetes
 * pod mounts an `emptyDir` at the same paths.
 */
class ReadOnlyRootFilesystemTest {
    companion object {
        private val TESTAPP_JAR: String = Objects.requireNonNull(
            System.getProperty("qubership.profiler.testapp.jar"),
            "system property qubership.profiler.testapp.jar"
        )

        private val CORE_BASE_IMAGE_TAG: String = Objects.requireNonNull(
            System.getProperty("qubership.profiler.java-base-image.tag"),
            "system property qubership.profiler.java-base-image.tag"
        )

        private const val FILES_START = "===NCDIAG_FILES_START==="
        private const val FILES_END = "===NCDIAG_FILES_END==="
    }

    @Test
    fun `agent writes dumps and applies custom config in ncdiag when the root filesystem is read-only`() {
        val container = GenericContainer(CORE_BASE_IMAGE_TAG)
            .withCreateContainerCmdModifier { it.hostConfig!!.withReadonlyRootfs(true) }
            .withTmpFs(
                mapOf(
                    "/tmp" to "rw",
                    "/etc/env" to "rw",
                    "/app/ncdiag" to "rw",
                    "/app/nss" to "rw",
                    "/etc/ssl/certs/java" to "rw",
                )
            )
            // Docker refuses to copy a file into a container whose root filesystem is read-only
            .withFileSystemBind(TESTAPP_JAR, "/app/testapp.jar", BindMode.READ_ONLY)
            .withEnv("ESC_LOG_LEVEL", "debug")
            .withEnv("PROFILER_ENABLED", "true")
            .withEnv("NC_DIAGNOSTIC_MODE", "prod")
            .withEnv("CLOUD_NAMESPACE", "test-namespace")
            .withEnv("MICROSERVICE_NAME", "test-app")
            .withEnv("X_JAVA_ARGS", "-Dcom.netcracker.profiler.Dumper.REMOTE_DUMP_DISABLED=true")
            .withEnv(
                "NC_DIAGNOSTIC_CUSTOM_CONFIG",
                """
                <profiler-configuration>
                    <store-transformed-classes>/app/ncdiag/transformed-classes</store-transformed-classes>
                </profiler-configuration>
                """.trimIndent()
            )
            // The entrypoint logs the command, so the markers are split by empty quotes to appear whole only in the output
            .withCommand(
                "sh", "-c",
                "java -jar /app/testapp.jar 3 && " +
                    "echo '${FILES_START.take(5)}''${FILES_START.drop(5)}' && " +
                    "find /app/ncdiag -type f && " +
                    "echo '${FILES_END.take(5)}''${FILES_END.drop(5)}'"
            )
            .withStartupAttempts(1)
            .withStartupTimeout(Duration.ofMinutes(2))
            .withLogConsumer(LogToConsolePrinter("[read-only-rootfs] "))
            .withStartupCheckStrategy(OneShotStartupCheckStrategy())

        val logs = container.use {
            it.start()
            it.logs
        }
        val files = logs.substringAfter(FILES_START).substringBefore(FILES_END)

        assertAll(
            {
                assertTrue(logs.contains("Profiler: initialized, version")) {
                    "The profiler did not start.\n\n$logs"
                }
            },
            {
                assertTrue(files.contains("/app/ncdiag/localdump/")) {
                    "No dump file under /app/ncdiag/localdump. Files under /app/ncdiag:\n$files\n\n$logs"
                }
            },
            {
                assertTrue(files.contains("/app/ncdiag/transformed-classes/")) {
                    "NC_DIAGNOSTIC_CUSTOM_CONFIG was not applied: no class under /app/ncdiag/transformed-classes. " +
                        "Files under /app/ncdiag:\n$files\n\n$logs"
                }
            },
        )
    }
}
