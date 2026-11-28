package org.qubership.profiler.diagtools

import com.netcracker.profiler.test.agent.LogToConsolePrinter
import org.junit.jupiter.api.Assertions.assertAll
import org.junit.jupiter.api.Assertions.assertEquals
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

    @Test
    fun `runtime data goes to a writable app ncdiag and the agent reads a copy of its config there`() {
        val folders = resolveFolders("mkdir /app/ncdiag")
        assertAll(
            { assertEquals("/app/diag", folders.home, "NC_DIAGNOSTIC_HOME") },
            { assertEquals("/app/ncdiag", folders.folder, "NC_DIAGNOSTIC_FOLDER") },
            { assertJavaArg(folders, "-javaagent:/app/diag/lib/agent.jar") },
            { assertJavaArg(folders, "-Dprofiler.home=/app/diag") },
            { assertJavaArg(folders, "-Dprofiler.config=/app/ncdiag/config/_config.xml") },
            { assertJavaArg(folders, "-Dprofiler.dump=/app/ncdiag/localdump") },
        )
    }

    @Test
    fun `an explicit NC_DIAGNOSTIC_FOLDER equal to the install folder keeps one folder`() {
        val folders = resolveFolders("mkdir /app/ncdiag", mapOf("NC_DIAGNOSTIC_FOLDER" to "/app/diag"))
        assertOneFolder(folders, "/app/diag")
    }

    @Test
    fun `a legacy PROFILER_FOLDER equal to the install folder keeps one folder`() {
        val folders = resolveFolders("mkdir /app/ncdiag", mapOf("PROFILER_FOLDER" to "/app/diag"))
        assertOneFolder(folders, "/app/diag")
    }

    @Test
    fun `settings and configuration left in app ncdiag by an earlier container are not used`() {
        // An emptyDir at /app/ncdiag outlives a container restart
        val folders = resolveFolders(
            // language=bash
            """
            mkdir -p /app/ncdiag/properties /app/ncdiag/config/default
            echo false > /app/ncdiag/properties/NC_DIAGNOSTIC_ESC_ENABLED
            echo '<profiler-configuration/>' > /app/ncdiag/config/default/custom-config.xml
            """.trimIndent(),
            probe = "ls /app/ncdiag/config/default",
        )
        assertAll(
            { assertJavaArg(folders, "-javaagent:/app/diag/lib/agent.jar") },
            { assertEquals("", folders.probe, "files in /app/ncdiag/config/default") },
        )
    }

    @Test
    fun `runtime data stays in the install folder when app ncdiag does not exist`() {
        val folders = resolveFolders("")
        assertOneFolder(folders, "/app/diag")
    }

    @Test
    fun `runtime data stays in the install folder when app ncdiag is not writable`() {
        // The OpenShift case: the image created /app/ncdiag, and the pod runs under another uid
        val folders = resolveFolders("mkdir /app/ncdiag && chown -R 1000:1000 /app/diag", runAsUid = 1000)
        assertOneFolder(folders, "/app/diag")
    }

    @Test
    fun `runtime data stays in the install folder when diag-lib predates NC_DIAGNOSTIC_HOME`() {
        val folders = resolveFolders("mkdir /app/ncdiag && cp /fixtures/diag-lib-before-home-split.sh /app/diag/diag-lib.sh")
        assertOneFolder(folders, "/app/diag")
    }

    @Test
    fun `the agent folder named by NC_DIAGNOSTIC_FOLDER is the home when the bootstrap script is elsewhere`() {
        val folders = resolveFolders(
            // language=bash
            """
            mkdir /app/ncdiag /opt/scripts
            cp /app/diag/diag-bootstrap.sh /app/diag/diag-lib.sh /opt/scripts/
            mv /app/diag /app/agent
            """.trimIndent(),
            mapOf("NC_DIAGNOSTIC_FOLDER" to "/app/agent"),
            bootstrap = "/opt/scripts/diag-bootstrap.sh",
        )
        assertOneFolder(folders, "/app/agent")
    }

    @Test
    fun `a dev mode update keeps runtime data in the install folder`() {
        // The collector may serve an agent.jar that runs diagtools from NC_DIAGNOSTIC_FOLDER.
        // The stubs stand for a successful download and unpack of that update.
        val folders = resolveFolders(
            // language=bash
            """
            mkdir /app/ncdiag
            printf '#!/bin/sh\necho 200\n' > /usr/local/bin/curl
            printf '#!/bin/sh\nexit 0\n' > /usr/local/bin/unzip
            chmod +x /usr/local/bin/curl /usr/local/bin/unzip
            """.trimIndent(),
            mapOf("NC_DIAGNOSTIC_MODE" to "dev"),
        )
        assertOneFolder(folders, "/app/diag")
    }

    @Test
    fun `a dev mode update that fails to unpack runs the installed agent in prod mode`() {
        val folders = resolveFolders(
            // language=bash
            """
            mkdir /app/ncdiag
            printf '#!/bin/sh\necho 200\n' > /usr/local/bin/curl
            printf '#!/bin/sh\nexit 1\n' > /usr/local/bin/unzip
            chmod +x /usr/local/bin/curl /usr/local/bin/unzip
            """.trimIndent(),
            mapOf("NC_DIAGNOSTIC_MODE" to "dev"),
        )
        assertAll(
            { assertEquals("prod", folders.mode, "NC_DIAGNOSTIC_MODE") },
            { assertEquals("/app/ncdiag", folders.folder, "NC_DIAGNOSTIC_FOLDER") },
            { assertJavaArg(folders, "-javaagent:/app/diag/lib/agent.jar") },
        )
    }

    private class Folders(
        val home: String,
        val folder: String,
        val mode: String,
        val javaArgs: String,
        val probe: String,
    )

    private fun assertJavaArg(folders: Folders, arg: String) {
        assertTrue(folders.javaArgs.split(' ').contains(arg)) { "X_JAVA_ARGS lacks $arg: ${folders.javaArgs}" }
    }

    private fun assertOneFolder(folders: Folders, folder: String) {
        assertAll(
            { assertEquals(folder, folders.home, "NC_DIAGNOSTIC_HOME") },
            { assertEquals(folder, folders.folder, "NC_DIAGNOSTIC_FOLDER") },
            { assertJavaArg(folders, "-javaagent:$folder/lib/agent.jar") },
            { assertJavaArg(folders, "-Dprofiler.dump=$folder/localdump") },
            {
                assertFalse(folders.javaArgs.contains("-Dprofiler.config")) {
                    "X_JAVA_ARGS should use the config in $folder: ${folders.javaArgs}"
                }
            },
        )
    }

    /**
     * Sources the bootstrap in a container where `/app/diag` holds the scripts, a `diagtools` stub, an empty
     * `lib/agent.jar` and `config/_config.xml`, after running [setup] as root.
     * [Folders.probe] is the output of [probe], run after the bootstrap, with lines joined by spaces.
     */
    private fun resolveFolders(
        setup: String,
        extraEnv: Map<String, String> = emptyMap(),
        bootstrap: String = "/app/diag/diag-bootstrap.sh",
        runAsUid: Int? = null,
        probe: String = "true",
    ): Folders {
        val runAs = if (runAsUid == null) "" else "setpriv --reuid=$runAsUid --regid=$runAsUid --clear-groups "
        val cmd =
            // language=bash
            """
            set -e
            mkdir -p /app/diag/lib /app/diag/config/default
            touch /app/diag/lib/agent.jar
            echo '<profiler-configuration/>' > /app/diag/config/_config.xml
            printf '#!/bin/sh\nexit 0\n' > /app/diag/diagtools
            chmod +x /app/diag/diagtools
            $setup
            ${runAs}bash -c '
              . $bootstrap
              echo "===HOME=${'$'}{NC_DIAGNOSTIC_HOME}"
              echo "===FOLDER=${'$'}{NC_DIAGNOSTIC_FOLDER}"
              echo "===MODE=${'$'}{NC_DIAGNOSTIC_MODE}"
              echo "===ARGS=${'$'}{X_JAVA_ARGS}"
              echo "===PROBE=${'$'}(echo ${'$'}($probe))"
            '
            """.trimIndent()
        val logs = bootstrapContainer(cmd, mapOf("DIAGNOSTIC_CENTER_DUMPS_ENABLED" to "false") + extraEnv)
        fun value(name: String) =
            logs.lineSequence().firstOrNull { it.startsWith("===$name=") }?.substringAfter("===$name=")
                ?: throw AssertionError("The bootstrap printed no $name.\n\n$logs")
        return Folders(value("HOME"), value("FOLDER"), value("MODE"), value("ARGS"), value("PROBE"))
    }

    private fun runBootstrap(extraEnv: Map<String, String>): String {
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

        val logs = bootstrapContainer(
            cmd,
            mapOf(
                "NC_DIAGNOSTIC_FOLDER" to "/app/diag",
                "CONSUL_ENABLED" to "true",
                "CONSUL_URL" to "http://consul:8500",
                "DIAGNOSTIC_CENTER_DUMPS_ENABLED" to "false",
            ) + extraEnv
        )
        return logs.substringAfter("===CALLS_START===").substringBefore("===CALLS_END===")
    }

    /** Runs [cmd] with the bootstrap scripts in `/app/diag` and returns the container log. */
    private fun bootstrapContainer(cmd: String, env: Map<String, String>): String {
        val bootstrapScript = File("scripts/diag-bootstrap.sh").absoluteFile
        val libScript = File("scripts/diag-lib.sh").absoluteFile
        val oldLibScript = File("src/test/resources/diag-lib-before-home-split.sh").absoluteFile
        require(bootstrapScript.isFile) { "diag-bootstrap script not found at $bootstrapScript" }
        require(libScript.isFile) { "diag-lib script not found at $libScript" }
        require(oldLibScript.isFile) { "older diag-lib script not found at $oldLibScript" }

        return GenericContainer("eclipse-temurin:21-jdk")
            .withLogConsumer(LogToConsolePrinter("[diag-bootstrap-test] "))
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
            .withCopyFileToContainer(
                MountableFile.forHostPath(oldLibScript.absolutePath, 0b110_100_100),
                "/fixtures/diag-lib-before-home-split.sh"
            )
            .withCommand("bash", "-c", cmd)
            .withEnv(env)
            .use { container ->
                container.start()
                container.logs
            }
    }
}
