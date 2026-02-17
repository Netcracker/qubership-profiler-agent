package org.qubership.profiler.diagtools

import com.netcracker.profiler.test.agent.LogToConsolePrinter
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.startupcheck.OneShotStartupCheckStrategy
import org.testcontainers.utility.MountableFile
import java.io.File
import java.time.Duration

class ThreadDumpTest {
    @Test
    fun threadDump() {
        val binary = File(System.getProperty("diagtools.binary"))
        require(binary.isFile) { "diagtools binary not found at $binary" }

        GenericContainer("eclipse-temurin:21-jdk")
            .withLogConsumer(LogToConsolePrinter("[diagtools] "))
            .withEnv("DIAGNOSTIC_CENTER_DUMPS_ENABLED", "false")
            .withEnv("LOG_TO_CONSOLE", "true")
            .withStartupCheckStrategy(
                OneShotStartupCheckStrategy().withTimeout(Duration.ofSeconds(30))
            )
            .withCopyFileToContainer(
                MountableFile.forHostPath(binary.absolutePath, 0b111_101_101),
                "/usr/local/bin/diagtools"
            )
            .withCommand(
                "sh", "-c",
                // language=bash
                """
                mkdir -p /tmp/diagnostic/log /app/ncdiag
                cat > /tmp/Sleep.java << 'JAVA'
                public class Sleep {
                    public static void main(String[] args) throws Exception {
                        Thread.sleep(300000);
                    }
                }
                JAVA
                javac /tmp/Sleep.java -d /tmp
                java -cp /tmp Sleep &
                sleep 3
                diagtools dump
                echo '===FILE_CONTENT_START==='
                cat /tmp/diagnostic/*.td.txt
                echo '===FILE_CONTENT_END==='
                """.trimIndent()
            )
            .use { container ->
                container.start()

                val logs = container.logs
                val fileContent =
                    logs.substringAfter("===FILE_CONTENT_START===").substringBefore("===FILE_CONTENT_END===")
                assertTrue(fileContent.contains("\"main\"")) {
                    """
                    Thread dump file should contain the "main" thread.
                    File content:
                    $fileContent
                    """.trimIndent()
                }
                assertTrue(fileContent.contains("java.lang.Thread.sleep")) {
                    """
                    Thread dump file should contain java.lang.Thread.sleep.
                    File content: $fileContent
                    """.trimIndent()
                }
            }
    }
}
