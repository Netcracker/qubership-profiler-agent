package com.netcracker.profiler.test.agent

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.images.builder.Transferable
import org.testcontainers.utility.MountableFile
import java.util.Objects

/**
 * E2E test that verifies diagtools collects GC logs and uploads them
 * to a WebDAV server (nginx), matching the production architecture.
 *
 * 1. Starts an nginx container with WebDAV enabled (same as dumps-collector in production)
 * 2. Starts a Java app container using the profiler base image
 *    - JVM writes GC logs via -Xlog:gc (configured by diag-lib.sh)
 *    - diagtools schedule picks up GC logs on scan tick and PUTs them to nginx
 * 3. Triggers GC activity so the gc.log keeps growing between scan ticks
 * 4. Verifies that the gc.log on nginx matches the original gc.log from the app container
 */
class GcLogCollectionTest {
    companion object {
        private val TESTAPP_JAR: String = Objects.requireNonNull(
            System.getProperty("qubership.profiler.testapp.jar"),
            "system property qubership.profiler.testapp.jar"
        )

        private val CORE_BASE_IMAGE_TAG: String = Objects.requireNonNull(
            System.getProperty("qubership.profiler.java-base-image.tag"),
            "system property qubership.profiler.java-base-image.tag"
        )

        // language=nginx
        private val NGINX_WEBDAV_CONFIG = """
            worker_processes 1;
            error_log /var/log/nginx/error.log info;
            pid /tmp/nginx.pid;
            events { worker_connections 128; }
            http {
                server {
                    listen 8080;
                    location /diagnostic/ {
                        root /data;
                        dav_methods PUT DELETE;
                        create_full_put_path on;
                    }
                }
            }
        """.trimIndent()
    }

    @Test
    fun gcLogIsUploaded() {
        val network = Network.newNetwork()

        // Nginx WebDAV receiver (same role as dumps-collector in production)
        val nginx = GenericContainer("nginx:alpine")
            .withNetwork(network)
            .withNetworkAliases("webdav.test-ns")
            .withExposedPorts(8080)
            .withCopyToContainer(
                Transferable.of(NGINX_WEBDAV_CONFIG),
                "/etc/nginx/nginx.conf"
            )
            .withCommand(
                "sh", "-c",
                "mkdir -p /data/diagnostic && chmod 777 /data/diagnostic && nginx -g 'daemon off;'"
            )
            .withLogConsumer(LogToConsolePrinter("[nginx] "))
            .waitingFor(Wait.forHttp("/").forPort(8080).forStatusCode(404))

        // Java app with profiler base image.
        // The entrypoint sources diag-bootstrap.sh which:
        //   1. Configures GC logging JVM args via diag-lib.sh
        //   2. Starts "diagtools schedule &" in background
        // Then the user command (java -jar testapp.jar 120) runs for 120s,
        // giving diagtools enough time to collect and upload GC logs.
        val app = GenericContainer(CORE_BASE_IMAGE_TAG)
            .withNetwork(network)
            .withEnv("NC_DIAGNOSTIC_MODE", "prod")
            .withEnv("NC_DIAGNOSTIC_AGENT_SERVICE", "http://webdav.test-ns:8080")
            .withEnv("CLOUD_NAMESPACE", "test-ns")
            .withEnv("MICROSERVICE_NAME", "test-app")
            .withEnv("DIAGNOSTIC_SCAN_INTERVAL", "5s")
            .withEnv("DIAGNOSTIC_DUMP_INTERVAL", "60m")
            .withEnv("ESC_LOG_LEVEL", "debug")
            .withEnv("NC_DIAGNOSTIC_ESC_ENABLED", "false")
            .withEnv("LOG_TO_CONSOLE", "true")
            .withCopyToContainer(MountableFile.forHostPath(TESTAPP_JAR), "/app/testapp.jar")
            .withCommand("java", "-jar", "/app/testapp.jar", "120")
            .withLogConsumer(LogToConsolePrinter("[app] "))

        network.use {
            nginx.use { nginx ->
                nginx.start()

                app.use { app ->
                    app.start()

                    // Wait for the first gc.log upload, so we know diagtools is working.
                    waitForGcLogFiles(nginx, minFiles = 1)

                    // Trigger GC activity so the gc.log grows between scan ticks.
                    // The JVM is configured with -Xlog:gc=trace, so each GC.run
                    // produces trace output in gc.log.
                    repeat(3) {
                        app.execInContainer("jcmd", "0", "GC.run")
                        Thread.sleep(6_000)
                    }

                    // Now we should have a gc.log on nginx that is a prefix of the original.
                    // diagtools uploads the full file each time, overwriting the same URL.
                    val gcFiles = findGcLogFiles(nginx)
                    assertTrue(gcFiles.isNotEmpty()) {
                        "Expected at least 1 gc.log file on WebDAV"
                    }

                    // Read the latest uploaded gc.log (there should be exactly one at any given URL)
                    val uploadedContent = gcFiles.sorted().last().let { file ->
                        nginx.execInContainer("cat", file).stdout
                    }

                    // Read the original gc.log from the app container
                    val originalGcLog = app.execInContainer(
                        "cat", "/tmp/diagnostic/gclogs/gc.log"
                    ).stdout

                    assertTrue(uploadedContent.isNotEmpty()) {
                        "Uploaded gc.log content should not be empty"
                    }

                    // The uploaded file should be a prefix of the original gc.log:
                    // the JVM may have written more data after the last diagtools scan tick.
                    assertTrue(originalGcLog.startsWith(uploadedContent)) {
                        val diffIdx = uploadedContent.zip(originalGcLog)
                            .indexOfFirst { (a, b) -> a != b }
                        """
                        Uploaded gc.log should be a prefix of the original gc.log.
                        Uploaded ${uploadedContent.length} chars,
                        original is ${originalGcLog.length} chars.
                        First difference at index $diffIdx.

                        Uploaded (first 500 chars):
                        ${uploadedContent.take(500)}

                        Original (first 500 chars):
                        ${originalGcLog.take(500)}
                        """.trimIndent()
                    }
                }
            }
        }
    }

    /**
     * Verifies that JVM log rotation (via `jcmd VM.log rotate`) produces rotated
     * gc.log.N files that diagtools uploads with distinct names, and that
     * the rotated files are deleted from the app container after upload.
     */
    @Test
    fun rotatedGcLogsAreUploadedWithDistinctNames() {
        val network = Network.newNetwork()

        val nginx = GenericContainer("nginx:alpine")
            .withNetwork(network)
            .withNetworkAliases("webdav.test-ns")
            .withExposedPorts(8080)
            .withCopyToContainer(
                Transferable.of(NGINX_WEBDAV_CONFIG),
                "/etc/nginx/nginx.conf"
            )
            .withCommand(
                "sh", "-c",
                "mkdir -p /data/diagnostic && chmod 777 /data/diagnostic && nginx -g 'daemon off;'"
            )
            .withLogConsumer(LogToConsolePrinter("[nginx] "))
            .waitingFor(Wait.forHttp("/").forPort(8080).forStatusCode(404))

        val app = GenericContainer(CORE_BASE_IMAGE_TAG)
            .withNetwork(network)
            .withEnv("NC_DIAGNOSTIC_MODE", "prod")
            .withEnv("NC_DIAGNOSTIC_AGENT_SERVICE", "http://webdav.test-ns:8080")
            .withEnv("CLOUD_NAMESPACE", "test-ns")
            .withEnv("MICROSERVICE_NAME", "test-app")
            .withEnv("DIAGNOSTIC_SCAN_INTERVAL", "5s")
            .withEnv("DIAGNOSTIC_DUMP_INTERVAL", "60m")
            .withEnv("ESC_LOG_LEVEL", "debug")
            .withEnv("NC_DIAGNOSTIC_ESC_ENABLED", "false")
            .withEnv("LOG_TO_CONSOLE", "true")
            .withCopyToContainer(MountableFile.forHostPath(TESTAPP_JAR), "/app/testapp.jar")
            .withCommand("java", "-jar", "/app/testapp.jar", "120")
            .withLogConsumer(LogToConsolePrinter("[app] "))

        network.use {
            nginx.use { nginx ->
                nginx.start()

                app.use { app ->
                    app.start()

                    // Wait for initial gc.log upload
                    waitForGcLogFiles(nginx, minFiles = 1)

                    // Trigger 3 JVM log rotations via jcmd.
                    // Each rotation renames gc.log → gc.log.0 (shifting existing .0→.1 etc.)
                    // and creates a fresh gc.log.
                    // Between rotations, wait for diagtools to pick up the rotated files.
                    val rotationCount = 3
                    repeat(rotationCount) { i ->
                        app.execInContainer("jcmd", "0", "VM.log", "rotate")
                        // Generate some GC output so the new gc.log is non-empty
                        app.execInContainer("jcmd", "0", "GC.run")
                        Thread.sleep(7_000) // wait for diagtools scan tick (5s) + margin
                    }

                    // Collect all uploaded files from nginx
                    val allFiles = findAllGcFiles(nginx)
                    val activeGcLogs = allFiles.filter { it.endsWith("/gc.log") }
                    val rotatedGcLogs = allFiles.filter { it.matches(Regex(".*/gc\\.log\\.\\d+$")) }

                    // There should be at least 1 active gc.log
                    assertTrue(activeGcLogs.isNotEmpty()) {
                        "Expected at least 1 active gc.log on WebDAV, found: $allFiles"
                    }

                    // There should be rotated gc.log.N files uploaded
                    assertTrue(rotatedGcLogs.size >= rotationCount) {
                        "Expected at least $rotationCount rotated gc.log.N files, " +
                            "found ${rotatedGcLogs.size}: $rotatedGcLogs\n" +
                            "All files: $allFiles"
                    }

                    // All rotated files should have distinct full paths
                    // (each is uploaded under a unique timestamp directory)
                    assertEquals(rotatedGcLogs.size, rotatedGcLogs.toSet().size) {
                        "Rotated gc.log files should have distinct paths: $rotatedGcLogs"
                    }

                    // Rotated files should be deleted from the app container after upload
                    val remaining = app.execInContainer(
                        "sh", "-c", "ls /tmp/diagnostic/gclogs/gc.log.* 2>/dev/null || true"
                    ).stdout.trim()
                    assertTrue(remaining.isEmpty()) {
                        "Rotated gc.log.* files should be cleaned up from app container, " +
                            "but found: $remaining"
                    }
                }
            }
        }
    }

    /**
     * Waits up to 60s for at least [minFiles] gc.log files on the WebDAV server.
     */
    private fun waitForGcLogFiles(nginx: GenericContainer<*>, minFiles: Int): List<String> {
        val deadline = System.currentTimeMillis() + 60_000
        var gcFiles = listOf<String>()
        while (System.currentTimeMillis() < deadline) {
            gcFiles = findGcLogFiles(nginx)
            if (gcFiles.size >= minFiles) return gcFiles
            Thread.sleep(2_000)
        }
        throw AssertionError(
            "Expected at least $minFiles gc.log file(s) on WebDAV, " +
                "but found ${gcFiles.size} after 60s.\n\n" +
                "Nginx logs (last 2000 chars):\n${nginx.logs.takeLast(2000)}"
        )
    }

    /**
     * Finds gc.log files (active only) uploaded to the nginx WebDAV storage.
     */
    private fun findGcLogFiles(nginx: GenericContainer<*>): List<String> {
        val result = nginx.execInContainer(
            "find", "/data/diagnostic", "-name", "gc.log", "-type", "f"
        )
        return result.stdout.lines().filter { it.isNotBlank() }
    }

    /**
     * Finds all gc.log* files (active + rotated) uploaded to the nginx WebDAV storage.
     */
    private fun findAllGcFiles(nginx: GenericContainer<*>): List<String> {
        val result = nginx.execInContainer(
            "find", "/data/diagnostic", "-name", "gc.log*", "-type", "f"
        )
        return result.stdout.lines().filter { it.isNotBlank() }
    }
}
