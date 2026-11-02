package com.netcracker.profiler.test.spring

import com.github.dockerjava.api.exception.NotFoundException
import com.google.inject.Guice
import com.netcracker.profiler.cli.DumpTrace
import com.netcracker.profiler.guice.ParsersModule
import net.sourceforge.argparse4j.inf.Namespace
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.images.builder.ImageFromDockerfile
import org.testcontainers.postgresql.PostgreSQLContainer
import java.io.File
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Duration

/**
 * The profiler agent records incoming HTTP, JDBC, and outgoing HTTP calls of a Spring Boot 4.1 application on
 * embedded Tomcat 11, on both the JDK 21 and the JDK 25 profiler base images, and reports no instrumentation or
 * plugin failure.
 *
 * The test layers the `apps/examples/spring-boot-4-tomcat` jar on the base image, starts it next to PostgreSQL,
 * and sends one request each to `/demo/db` and `/demo/outgoing`. The agent writes its dump inside the container;
 * the test copies the dump out and reads it back with the CLI's [DumpTrace], which prints one TSV row per trace
 * event. It then checks the agent's own log with [agentFailureLines].
 *
 * Jetty and WebFlux variants are not covered yet: they wait on Jetty 12.1 support (issue #1233) and on hooks for
 * `RestClient` and the other Boot 4 HTTP clients (issue #1235).
 */
class SpringBoot4TomcatProfilingTest {
    private class Marker(val description: String, val matches: (TraceRow) -> Boolean)

    private class TraceRow(val eventType: String, val tag: String, val value: String, val line: String)

    @ParameterizedTest
    @ValueSource(strings = ["jdk21", "jdk25"])
    fun `agent records incoming HTTP, JDBC, and outgoing HTTP calls`(jdk: String, @TempDir tempDir: Path) {
        val demoJar: Path = Paths.get(System.getProperty("test.demoAppJar"))
        val baseImageTag = System.getProperty("test.baseImageTag.$jdk")

        val image = ImageFromDockerfile("qubership-profiler-it-spring-boot-4-$jdk", false)
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
                        // Route the agent's own (isolated) Logback to a file the test reads back
                        "-Dprofiler.log.config=/app/agent-logback.xml",
                        // Write the dump to a local directory instead of sending it to a collector
                        "-Dprofiler.dump=/app/diag/localdump/podname",
                        "-Dcom.netcracker.profiler.Dumper.REMOTE_DUMP_DISABLED=true",
                        "-jar",
                        "/app/app.jar",
                    )
                    .build()
            }

        Network.newNetwork().use { network ->
            PostgreSQLContainer("postgres:17-alpine")
                .withNetwork(network)
                .withNetworkAliases("postgres")
                .use { postgres ->
                    postgres.start()
                    GenericContainer(image).use { app ->
                        app.withNetwork(network)
                            .withEnv("SPRING_DATASOURCE_URL", "jdbc:postgresql://postgres:5432/${postgres.databaseName}")
                            .withEnv("SPRING_DATASOURCE_USERNAME", postgres.username)
                            .withEnv("SPRING_DATASOURCE_PASSWORD", postgres.password)
                            .withExposedPorts(8080)
                            .waitingFor(
                                Wait.forHttp("/demo/ping")
                                    .forStatusCode(200)
                                    .withStartupTimeout(Duration.ofMinutes(2)),
                            )
                        app.start()

                        val baseUrl = "http://${app.host}:${app.getMappedPort(8080)}"
                        assertGet(baseUrl, "/demo/db", "profiler-it-sb4")
                        assertGet(baseUrl, "/demo/outgoing", "pong")

                        val rows = awaitTrace(app, tempDir)
                        val pluginExceptions = rows.filter { it.tag == "profiler.plugin.exception" }
                        assertEquals(
                            emptyList<String>(),
                            pluginExceptions.map { it.line },
                            "profiler.plugin.exception rows in the dump",
                        )

                        assertEquals(emptyList<String>(), agentFailureLines(readAgentLog(app)), "agent log failures")
                    }
                }
        }
    }

    private fun assertGet(baseUrl: String, path: String, expectedBody: String) {
        val response = HttpClient.newHttpClient().send(
            HttpRequest.newBuilder(URI.create(baseUrl + path)).GET().build(),
            HttpResponse.BodyHandlers.ofString(),
        )
        assertEquals(200 to expectedBody, response.statusCode() to response.body(), "GET $path")
    }

    /**
     * Polls the agent's dump until every marker in [MARKERS] is present, and returns the rows of the last read.
     * Fails with the missing markers and the relevant rows when the dump is incomplete after [TRACE_TIMEOUT].
     */
    private fun awaitTrace(app: GenericContainer<*>, tempDir: Path): List<TraceRow> {
        val deadline = System.nanoTime() + TRACE_TIMEOUT.toNanos()
        var rows = emptyList<TraceRow>()
        var missing = MARKERS.map { it.description }
        var lastCopyError: NotFoundException? = null
        var lastReaderCode: Int? = null
        var attempt = 0
        while (true) {
            attempt++
            val target = tempDir.resolve("poll-$attempt")
            try {
                copyDirectoryFromContainer(app, DUMP_DIR, target)
                val dumpRoot = target.resolve(DUMP_DIR.substringAfterLast('/')).toFile()
                val result = dumpTrace(dumpRoot, tempDir, attempt)
                if (result == null) {
                    // DumpTrace logs the exception; a trace file whose header is not flushed yet ends here too
                    lastReaderCode = -1
                } else {
                    rows = result
                    missing = MARKERS.filterNot { marker -> rows.any(marker.matches) }.map { it.description }
                    if (missing.isEmpty()) {
                        return rows
                    }
                }
            } catch (e: NotFoundException) {
                // The agent has not created the dump directory yet
                lastCopyError = e
            }
            if (System.nanoTime() > deadline) {
                break
            }
            Thread.sleep(POLL_INTERVAL.toMillis())
        }
        val relevant = rows.filter { row -> RELEVANT_TEXT.any { row.tag.contains(it) } }
        return fail(
            buildString {
                appendLine("Timed out after ${TRACE_TIMEOUT.seconds} s waiting for the agent's dump in $DUMP_DIR")
                appendLine("Missing: $missing")
                lastCopyError?.let { appendLine("Last copy error: $it") }
                lastReaderCode?.let { appendLine("DumpTrace.accept returned $it on at least one poll, see its log") }
                appendLine("Last ${minOf(relevant.size, 200)} of ${relevant.size} relevant rows out of ${rows.size}:")
                relevant.takeLast(200).forEach { appendLine(it.line) }
            },
        )
    }

    /**
     * Returns one row per trace event under [dumpRoot], or `null` when [DumpTrace.accept] reports a read error.
     */
    private fun dumpTrace(dumpRoot: File, tempDir: Path, attempt: Int): List<TraceRow>? {
        val tsv = tempDir.resolve("trace-$attempt.tsv")
        val dumpTrace = Guice.createInjector(ParsersModule(dumpRoot)).getInstance(DumpTrace::class.java)
        val code = dumpTrace.accept(
            Namespace(
                mapOf(
                    "output_file" to tsv.toString(),
                    "start_date" to "1hour",
                    "end_date" to "now",
                    "time_zone" to "UTC",
                    "server" to null,
                    "contains" to null,
                ),
            ),
        )
        if (code == -1) {
            return null
        }
        assertEquals(0, code, "DumpTrace.accept($dumpRoot)")
        val lines = Files.readAllLines(tsv)
        val header = lines.first().split('\t')
        val eventType = header.indexOf("eventType")
        val tag = header.indexOf("tag")
        val value = header.indexOf("value")
        return lines.drop(1).map { line ->
            val columns = line.split('\t')
            TraceRow(columns[eventType], columns[tag], columns.getOrElse(value) { "" }, line)
        }
    }

    private fun copyDirectoryFromContainer(app: GenericContainer<*>, containerPath: String, target: Path) {
        val root = target.toAbsolutePath().normalize()
        Files.createDirectories(root)
        app.dockerClient.copyArchiveFromContainerCmd(app.containerId, containerPath).exec().use { stream ->
            TarArchiveInputStream(stream).use { tar ->
                while (true) {
                    val entry = tar.nextEntry ?: break
                    val file = root.resolve(entry.name).normalize()
                    require(file.startsWith(root)) { "Tar entry ${entry.name} resolves outside $root" }
                    if (entry.isDirectory) {
                        Files.createDirectories(file)
                    } else {
                        Files.createDirectories(file.parent)
                        Files.copy(tar, file)
                    }
                }
            }
        }
    }

    private fun readAgentLog(app: GenericContainer<*>): String =
        // Logback's FileAppender creates the file when the agent starts, so a failed copy fails the test
        app.copyFileFromContainer(AGENT_LOG) { it.readBytes().toString(Charsets.UTF_8) }

    private companion object {
        const val DUMP_DIR = "/app/diag/localdump"
        const val AGENT_LOG = "/tmp/agent-plugin.log"
        val TRACE_TIMEOUT: Duration = Duration.ofSeconds(60)
        val POLL_INTERVAL: Duration = Duration.ofSeconds(2)

        val MARKERS = listOf(
            Marker("enter StandardEngineValve.invoke") {
                it.eventType == "enter" && it.tag.contains("org.apache.catalina.core.StandardEngineValve.invoke")
            },
            Marker("web.url /demo/db") { it.eventType == "tag" && it.tag == "web.url" && it.value.contains("/demo/db") },
            Marker("web.url /demo/outgoing") {
                it.eventType == "tag" && it.tag == "web.url" && it.value.contains("/demo/outgoing")
            },
            Marker("enter QueryExecutorImpl.execute") {
                it.eventType == "enter" && it.tag.contains("org.postgresql.core.v3.QueryExecutorImpl.execute")
            },
            Marker("sql tag") { it.eventType == "tag" && it.tag == "sql" },
            Marker("resttemplate.url /demo/ping") {
                it.eventType == "tag" && it.tag == "resttemplate.url" && it.value.contains("/demo/ping")
            },
            Marker("enter HttpClient.parseHTTP") {
                it.eventType == "enter" && it.tag.contains("sun.net.www.http.HttpClient.parseHTTP")
            },
        )

        val RELEVANT_TEXT = listOf(
            "StandardEngineValve",
            "web.url",
            "QueryExecutorImpl",
            "sql",
            "resttemplate",
            "parseHTTP",
            "profiler.plugin.exception",
        )
    }
}
