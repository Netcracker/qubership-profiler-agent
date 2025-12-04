package com.netcracker.profiler.test.agent;

import static org.junit.jupiter.api.Assertions.*;

import com.netcracker.profiler.cloud.transport.ProtocolConst;
import com.netcracker.profiler.collector.mock.MockCollectorServer;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.search.RequiredSearch;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.startupcheck.OneShotStartupCheckStrategy;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;

@Testcontainers
public class JavaBaseImageTest {
    private static final String TESTAPP_JAR =
            Objects.requireNonNull(
                    System.getProperty("qubership.profiler.testapp.jar"),
                    "system property qubership.profiler.testapp.jar");

    private static final String CORE_BASE_IMAGE_TAG =
            Objects.requireNonNull(
                    System.getProperty("qubership.profiler.java-base-image.tag"),
                    "system property qubership.profiler.java-base-image.tag");

    @Test
    void profilerSendsDataToMockCollector() {
        try (MockCollectorServer mockCollector =
                     new MockCollectorServer(0, ProtocolConst.PLAIN_SOCKET_BACKLOG)
                             .started(Duration.ofSeconds(5));
             GenericContainer<?> profilerApp = new GenericContainer<>(CORE_BASE_IMAGE_TAG)
                     .withEnv("ESC_LOG_LEVEL", "debug")
                     .withEnv("PROFILER_ENABLED", "true")
                     .withEnv("REMOTE_DUMP_HOST", "host.testcontainers.internal")
                     .withEnv("REMOTE_DUMP_PORT_PLAIN", String.valueOf(mockCollector.getPort()))
                     .withEnv("CLOUD_NAMESPACE", "test-namespace")
                     .withEnv("MICROSERVICE_NAME", "test-app")
                     .withCommand("java", "-jar", "/app/testapp.jar")
                     .withCopyToContainer(MountableFile.forHostPath(TESTAPP_JAR), "/app/testapp.jar")
                     .withStartupAttempts(1)
                     .withStartupTimeout(Duration.ofMinutes(1))
                     .withLogConsumer(new LogToConsolePrinter("[profilerApp] "))
                     .withStartupCheckStrategy(new OneShotStartupCheckStrategy())) {
            org.testcontainers.Testcontainers.exposeHostPorts(mockCollector.getPort());
            profilerApp.start();

            // Verify profiler agent was enabled
            String profilerLogs = profilerApp.getLogs(OutputFrame.OutputType.STDERR);
            assertTrue(
                    profilerLogs.contains("-javaagent:"),
                    "Profiler agent should be enabled");

            assertCounterNonZero(mockCollector.getMetricRegistry(), "mock.server.connections");
            assertCounterNonZero(mockCollector.getMetricRegistry(), "mock.server.stream.chunks", "stream_name", "posDictionary");
            assertCounterNonZero(mockCollector.getMetricRegistry(), "mock.server.stream.chunks", "stream_name", "trace");
            assertCounterNonZero(mockCollector.getMetricRegistry(), "mock.server.stream.chunks", "stream_name", "calls");
        }
    }

    private static void assertCounterNonZero(MeterRegistry registry, String metricName, String... tags) {
        RequiredSearch search = registry.get(metricName);
        if (tags.length > 0) {
            search = search.tags(tags);
        }
        Counter counter = search.counter();
        if (counter.count() > 0) {
            return;
        }
        fail("Expected counter " + metricName + " with tags " + Arrays.toString(tags) + " to be non-zero, but was zero");
    }
}
