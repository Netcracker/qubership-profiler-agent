package com.netcracker.profiler.test.spring

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

/**
 * The fixtures follow the layout of `agent-logback.xml`: `%-5level %logger - %msg%n%rEx`.
 */
class AgentLogCheckTest {
    @Test
    fun `a class that failed to instrument is reported`() {
        val log = """
            INFO  com.netcracker.profiler.agent.ProfilingTransformer - Instrumenting org.apache.catalina.core.StandardEngineValve
            WARN  com.netcracker.profiler.agent.ProfilingTransformer - Unable to instrument class org/apache/catalina/core/StandardEngineValve, java.lang.IllegalStateException: frame mismatch
        """.trimIndent()

        assertEquals(
            listOf(
                "WARN  com.netcracker.profiler.agent.ProfilingTransformer - Unable to instrument class org/apache/catalina/core/StandardEngineValve, java.lang.IllegalStateException: frame mismatch",
            ),
            agentFailureLines(log),
        )
    }

    @Test
    fun `a plugin exception is reported with its stack trace`() {
        val log = """
            INFO  com.netcracker.profiler.agent.Bootstrap - Profiler started
            ERROR com.netcracker.profiler.agent.plugins.ProfilerPluginLoggerImpl - Exception in profiler plugin
            java.lang.NoSuchFieldError: url
            	at sun.net.www.http.HttpClient.parseHTTP${'$'}profiler(HttpClient.java)
            	at sun.net.www.http.HttpClient.parseHTTP(HttpClient.java:710)
        """.trimIndent()

        assertEquals(
            listOf(
                "ERROR com.netcracker.profiler.agent.plugins.ProfilerPluginLoggerImpl - Exception in profiler plugin",
                "java.lang.NoSuchFieldError: url",
                "\tat sun.net.www.http.HttpClient.parseHTTP\$profiler(HttpClient.java)",
                "\tat sun.net.www.http.HttpClient.parseHTTP(HttpClient.java:710)",
            ),
            agentFailureLines(log),
        )
    }

    @Test
    fun `a missing if-enhancer filter is reported`() {
        val log = """
            WARN  com.netcracker.profiler.agent.ProfilingTransformer - Filter tomcat_filter is not found. Please check if-enhancer attributes
        """.trimIndent()

        assertEquals(
            listOf(
                "WARN  com.netcracker.profiler.agent.ProfilingTransformer - Filter tomcat_filter is not found. Please check if-enhancer attributes",
            ),
            agentFailureLines(log),
        )
    }

    @Test
    fun `a jar whose manifest cannot be read is reported`() {
        val log = """
            INFO  com.netcracker.profiler.agent.Bootstrap - Profiler started
            WARN  com.netcracker.profiler.instrument.enhancement.ClassInfoImpl - Unable to open jar file /app/app.jar/!BOOT-INF/lib/postgresql-42.7.14.jar
        """.trimIndent()

        assertEquals(
            listOf(
                "WARN  com.netcracker.profiler.instrument.enhancement.ClassInfoImpl - Unable to open jar file /app/app.jar/!BOOT-INF/lib/postgresql-42.7.14.jar",
            ),
            agentFailureLines(log),
        )
    }

    @Test
    fun `a configuration tag skipped by its condition is not reported`() {
        val log = """
            ERROR com.netcracker.profiler.configuration.ConfigurationImpl - Ignoring tag [profiler, enhancer] since condition did not match
        """.trimIndent()

        assertEquals(emptyList<String>(), agentFailureLines(log))
    }

    @Test
    fun `an info line that names a hooked class is not reported`() {
        val log = """
            INFO  com.netcracker.profiler.agent.ProfilingTransformer - Instrumenting org.apache.catalina.core.StandardEngineValve
        """.trimIndent()

        assertEquals(emptyList<String>(), agentFailureLines(log))
    }

    @Test
    fun `an empty log is not reported`() {
        assertEquals(emptyList<String>(), agentFailureLines(""))
    }
}
