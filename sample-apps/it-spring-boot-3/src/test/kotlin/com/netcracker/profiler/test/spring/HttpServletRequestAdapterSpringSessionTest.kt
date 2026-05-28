package com.netcracker.profiler.test.spring

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.images.builder.ImageFromDockerfile
import java.io.File
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Duration

/**
 * End-to-end reproduction of the production IllegalAccessException.
 *
 * Setup:
 *  - `backend/examples/spring-boot-3-undertow` is packaged by Maven into a Spring Boot fat jar.
 *  - The test layers that jar on top of `qubership/qubership-core-base-image:profiler-latest`
 *    (built by the `:installer:buildBaseImage` task). The base image already has the profiler
 *    agent installed under `/app/diag` and wires `-javaagent:/app/diag/lib/agent.jar` via its
 *    entrypoint when `NC_DIAGNOSTIC_MODE=prod`.
 *  - Testcontainers manages the container lifecycle, exposing a random host port.
 *
 * Why a real container:
 *  - The agent's bytecode instrumentation fires against the real Undertow
 *    `FilterHandler$FilterChainImpl.doFilter` — the exact call site from the production
 *    stacktrace. Same image, same agent layout, same JAVA_TOOL_OPTIONS as production.
 *
 * How the bug is detected:
 *  - The agent loads its OWN isolated Logback (via its PluginClassLoader), so plugin exceptions
 *    never reach the application's log. We point the agent at a dedicated Logback config through
 *    its supported `-Dprofiler.log.config` switch (test resource `agent-logback.xml`) that writes
 *    everything — including `ProfilerPluginLoggerImpl` errors — to `/tmp/agent-plugin.log`. After
 *    the request we copy that file out of the container and assert it carries no swallowed
 *    `IllegalAccessException` against `SessionRepositoryRequestWrapper`.
 *
 * See `installer-zip-test/.../PluginLoggerVisibilityTest` for the isolated characterization of why
 * the agent's Logback is separate from the application's.
 */
class HttpServletRequestAdapterSpringSessionTest {

    @Test
    fun `agent does not raise IllegalAccessException for Spring Session wrapped request`() {
        val demoJar: Path = Paths.get(System.getProperty("test.demoAppJar"))
        val baseImageTag = System.getProperty("test.baseImageTag")

        val image = ImageFromDockerfile("qubership-profiler-it-spring-boot-3", false)
            .withFileFromPath("app.jar", demoJar)
            .withFileFromClasspath("agent-logback.xml", "agent-logback.xml")
            .withDockerfileFromBuilder { b ->
                b
                    .from(baseImageTag)
                    .copy("app.jar", "/app/app.jar")
                    .copy("agent-logback.xml", "/app/agent-logback.xml")
                    .env("NC_DIAGNOSTIC_MODE", "prod")
                    .expose(8080)
                    .cmd(
                        "java",
                        // Route the agent's own (isolated) Logback to a file we can read back.
                        "-Dprofiler.log.config=/app/agent-logback.xml",
                        "-jar",
                        "/app/app.jar",
                    )
                    .build()
            }

        GenericContainer(image).use { container ->
            container.withExposedPorts(8080)
                .waitingFor(
                    Wait.forHttp("/health")
                        .forStatusCode(200)
                        .withStartupTimeout(Duration.ofMinutes(2)),
                )
            container.start()

            val baseUrl = "http://${container.host}:${container.getMappedPort(8080)}"
            val response = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create("$baseUrl/health")).GET().build(),
                HttpResponse.BodyHandlers.ofString(),
            )
            assertEquals(200, response.statusCode())

            // Let the agent's FileAppender flush before copying it out.
            Thread.sleep(500)

            val agentLog = File.createTempFile("agent-plugin-", ".log").apply { deleteOnExit() }
            try {
                container.copyFileFromContainer("/tmp/agent-plugin.log") { input ->
                    agentLog.outputStream().use { input.copyTo(it) }
                }
            } catch (_: Throwable) {
                // No file → agent logged nothing → no plugin error. Treat as empty.
                agentLog.writeText("")
            }

            val captured = agentLog.readText()
            val swallowed = captured.lineSequence()
                .filter {
                    it.contains("IllegalAccessException") &&
                            it.contains("SessionRepositoryRequestWrapper")
                }
                .toList()

            assertTrue(
                swallowed.isEmpty(),
                buildString {
                    appendLine("Profiler swallowed IllegalAccessException(s) for the Spring Session wrapper.")
                    appendLine("Matched lines (${swallowed.size}):")
                    swallowed.take(3).forEach { appendLine("  $it") }
                    appendLine()
                    appendLine("--- /tmp/agent-plugin.log (truncated to 4 KiB) ---")
                    appendLine(captured.takeLast(4096).ifEmpty { "(empty)" })
                },
            )
        }
    }
}
