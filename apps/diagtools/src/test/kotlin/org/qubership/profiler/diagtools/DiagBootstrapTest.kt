package org.qubership.profiler.diagtools

import com.netcracker.profiler.test.agent.LogToConsolePrinter
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.startupcheck.OneShotStartupCheckStrategy
import org.testcontainers.utility.MountableFile
import java.io.File
import java.time.Duration

class DiagBootstrapTest {
    @Test
    fun doesNotFetchExternalConfigWhenModeOff() {
        val output = runBootstrap(mapOf("NC_DIAGNOSTIC_MODE" to "off"))
        assertFalse(output.contains("consulCfg")) {
            "diagtools consulCfg should not be called when NC_DIAGNOSTIC_MODE=off. Calls:\n${output}"
        }
    }

    @Test
    fun fetchesExternalConfigWhenModeIsNotSet() {
        val output = runBootstrap(emptyMap())
        assertTrue(output.contains("consulCfg")) {
            "diagtools consulCfg should be called when mode is not set and consul is enabled. Calls:\n${output}"
        }
    }

    private fun runBootstrap(extraEnv: Map<String, String>): String {
        val bootstrapScript = File("scripts/diag-bootstrap.sh").absoluteFile
        val libScript = File("scripts/diag-lib.sh").absoluteFile
        require(bootstrapScript.isFile) { "diag-bootstrap script not found at $bootstrapScript" }
        require(libScript.isFile) { "diag-lib script not found at $libScript" }

        val cmd =
            // language=bash
            """
            set -e
            chmod +x /app/diag/diag-bootstrap.sh
            cat > /app/diag/diagtools << 'SH'
            #!/bin/sh
            echo "$@" >> /tmp/diagtools_calls.log
            exit 0
            SH
            chmod +x /app/diag/diagtools

            . /app/diag/diag-bootstrap.sh

            echo '===CALLS_START==='
            cat /tmp/diagtools_calls.log 2>/dev/null || true
            echo '===CALLS_END==='
            """.trimIndent()

        GenericContainer("eclipse-temurin:21-jdk")
            .withLogConsumer(LogToConsolePrinter("[diag-bootstrap-test] "))
            .withEnv("NC_DIAGNOSTIC_FOLDER", "/app/diag")
            .withEnv("CONSUL_ENABLED", "true")
            .withEnv("CONSUL_URL", "http://consul:8500")
            .withEnv("DIAGNOSTIC_CENTER_DUMPS_ENABLED", "false")
            .withStartupCheckStrategy(
                OneShotStartupCheckStrategy().withTimeout(Duration.ofSeconds(30))
            )
            .withCopyFileToContainer(
                MountableFile.forHostPath(bootstrapScript.absolutePath, 0b111_101_101),
                "/app/diag/diag-bootstrap.sh"
            )
            .withCopyFileToContainer(
                MountableFile.forHostPath(libScript.absolutePath, 0b111_101_101),
                "/app/diag/diag-lib.sh"
            )
            .withCommand("bash", "-c", cmd)
            .apply {
                for ((key, value) in extraEnv) {
                    withEnv(key, value)
                }
            }
            .use { container ->
                container.start()
                val logs = container.logs
                return logs.substringAfter("===CALLS_START===").substringBefore("===CALLS_END===")
            }
    }
}
