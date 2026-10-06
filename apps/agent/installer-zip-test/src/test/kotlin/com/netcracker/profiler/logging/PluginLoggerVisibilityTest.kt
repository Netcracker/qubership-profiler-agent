package com.netcracker.profiler.logging

import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.joran.JoranConfigurator
import com.netcracker.profiler.agent.Profiler
import com.netcracker.profiler.agent.ProfilerData
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNotSame
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import java.io.ByteArrayInputStream
import java.io.File
import java.nio.charset.StandardCharsets

/**
 * Minimal, isolated characterization of the plugin-logger visibility problem — no HTTP, no
 * Spring, no bytecode instrumentation, runs in ~1s.
 *
 * Symptom (seen in production and in the it-spring-boot-3 integration test): a swallowed plugin
 * exception routed through [Profiler.pluginException] never shows up in the host application's
 * log, so the `it-spring-boot-3` test has to rely on the fail-loud
 * `com.netcracker.profiler.agent.echoPluginExceptionToStderr` property instead.
 *
 * Root cause demonstrated here: the agent does NOT share Logback with the application. Its
 * `ProfilerPluginLoggerImpl` logger is created through the agent's own
 * `com.netcracker.profiler.agent.PluginClassLoader`, which loads a *separate* copy of
 * logback-classic with its *own* [LoggerContext]. Consequently:
 *  - reconfiguring the application's `LoggerContext` (exactly what Spring Boot's
 *    `LoggingApplicationListener` does on startup) has no effect on the agent's logger, and
 *  - plugin errors land only in the agent context's own appenders (the STDOUT appender from
 *    `profiler-home/config/logback.xml`), never in the appenders the application configured.
 *
 * The assertions below lock in that root cause. When the agent gains a deliberate, documented
 * bridge for surfacing plugin errors (e.g. routing them to the application's logging pipeline,
 * or the gated stderr echo becoming the supported channel), update this test accordingly.
 */
class PluginLoggerVisibilityTest {

    @Test
    fun `agent plugin logger is isolated from the application Logback context`() {
        // The agent must be attached and initialized, otherwise the scenario is meaningless.
        assertNotNull(
            ProfilerData.pluginLogger,
            "ProfilerData.pluginLogger is null — the profiler agent is not attached/initialized; " +
                "this test must run under -javaagent (installer-zip-test wires that up).",
        )

        val logFile = File.createTempFile("plugin-logger-visibility", ".log").apply { deleteOnExit() }

        // Reconfigure Logback the way a host application (Spring Boot) does on startup: reset the
        // global context and re-apply a config — here everything goes to a known file.
        val appContext = LoggerFactory.getILoggerFactory() as LoggerContext
        appContext.reset()
        JoranConfigurator().apply {
            context = appContext
            doConfigure(
                ByteArrayInputStream(
                    """
                    <configuration>
                      <appender name="FILE" class="ch.qos.logback.core.FileAppender">
                        <file>${logFile.absolutePath}</file>
                        <append>false</append>
                        <encoder><pattern>%-5level %logger - %msg%n%rEx</pattern></encoder>
                      </appender>
                      <root level="INFO">
                        <appender-ref ref="FILE"/>
                      </root>
                    </configuration>
                    """.trimIndent().toByteArray(StandardCharsets.UTF_8),
                ),
            )
        }

        // Note: the agent's LoggerContext is an instance of ch.qos.logback.classic.LoggerContext
        // loaded by the agent's PluginClassLoader — a DIFFERENT Class object than the
        // application's same-named LoggerContext (AppClassLoader). So we keep it as Any? and never
        // cast it to the application's type (that cast would itself throw ClassCastException —
        // which is, in fact, further proof of the isolation).
        val agentContext: Any? = agentLoggerContext()
        val agentContextClassLoader = agentContext?.javaClass?.classLoader
        println("[diag] application LoggerContext = ${id(appContext)} (CL ${cl(appContext.javaClass)})")
        println("[diag] agent       LoggerContext = ${id(agentContext)} (CL ${agentContextClassLoader ?: "bootstrap"})")
        println("[diag] agent pluginLogger        = ${id(ProfilerData.pluginLogger)}")

        // Fire the swallowed plugin exception through the agent's logger, then flush the app file.
        Profiler.pluginException(RuntimeException("sentinel-plugin-error"))
        appContext.stop()

        val captured = logFile.readText(StandardCharsets.UTF_8)
        println("[diag] captured app-file length  = ${captured.length}")

        // (1) The agent logs through a different LoggerContext than the application.
        assertNotNull(agentContext, "Could not introspect the agent's Logback LoggerContext")
        assertNotSame(
            appContext,
            agentContext,
            "Expected the agent to use a LoggerContext isolated from the application's, " +
                "but they are the same instance.",
        )
        // The isolation is rooted in a separate logback-classic copy: the agent's LoggerContext is
        // loaded by the agent's PluginClassLoader, not by the application classloader.
        assertNotSame(
            appContext.javaClass.classLoader,
            agentContextClassLoader,
            "Expected the agent's logback-classic to be loaded by a different classloader " +
                "than the application's, but both came from the same classloader.",
        )

        // (2) Therefore the application-configured appender never receives the plugin error.
        assertEquals(
            "",
            captured,
            "Plugin error unexpectedly reached the application-configured Logback appender. " +
                "If the agent now bridges into the application's logging pipeline, update this test. " +
                "Captured:\n$captured",
        )
    }

    /**
     * Reflectively obtains the Logback `LoggerContext` backing the agent's ProfilerPluginLoggerImpl
     * logger. Returned as [Any] on purpose: that context is a `ch.qos.logback.classic.LoggerContext`
     * loaded by the agent's PluginClassLoader, which is a different `Class` than this test's
     * same-named type, so it must not be cast.
     */
    private fun agentLoggerContext(): Any? {
        val pluginLogger = ProfilerData.pluginLogger ?: return null
        return runCatching {
            val loggerField = pluginLogger.javaClass.getDeclaredField("logger").apply { isAccessible = true }
            val slf4jLogger = loggerField.get(pluginLogger)
            val getContext = slf4jLogger.javaClass.methods.first { it.name == "getLoggerContext" }
            getContext.invoke(slf4jLogger)
        }.getOrNull()
    }

    private fun id(o: Any?): String =
        if (o == null) "null" else "${o.javaClass.name}@${System.identityHashCode(o).toString(16)}"

    private fun cl(c: Class<*>): String = c.classLoader?.toString() ?: "bootstrap"
}
