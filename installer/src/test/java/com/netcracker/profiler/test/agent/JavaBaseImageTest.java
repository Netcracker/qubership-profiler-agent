package com.netcracker.profiler.test.agent;

import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.startupcheck.OneShotStartupCheckStrategy;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
public class JavaBaseImageTest {
    private static final String BASE_IMAGE_BUILD_ROOT = System.getProperty("qubership.profiler.test.java-base.root");
    private static final String TESTAPP_JAR = System.getProperty("qubership.profiler.testapp.jar");

    static {
        assertNotNull(BASE_IMAGE_BUILD_ROOT, "qubership.profiler.test.java-base.root system property must be set");
    }

    @AutoClose
    Network network = Network.newNetwork();

    @Container
    MockCollectorContainer mockCollector = new MockCollectorContainer()
            .withNetwork(network)
            .withNetworkAliases("mock-collector");

    @Container
    GenericContainer<?> profilerApp = new GenericContainer<>("qubership/qubership-core-base-image:profiler-latest")
            .dependsOn(mockCollector)
            .withNetwork(network)
            .withEnv("ESC_LOG_LEVEL", "debug")
            .withEnv("PROFILER_ENABLED", "true")
            .withEnv("REMOTE_DUMP_HOST", "mock-collector")
            .withEnv("REMOTE_DUMP_PORT_PLAIN", String.valueOf(mockCollector.getCollectorPort()))
            .withEnv("CLOUD_NAMESPACE", "test-namespace")
            .withEnv("MICROSERVICE_NAME", "test-app")
            .withCommand("java", "-jar", "/app/testapp.jar")
            .withCopyToContainer(MountableFile.forHostPath(TESTAPP_JAR), "/app/testapp.jar")
            .withStartupAttempts(1)
            .withStartupTimeout(Duration.ofMinutes(1))
            .withLogConsumer(new LogToConsolePrinter("[profilerApp] "))
            .withStartupCheckStrategy(new OneShotStartupCheckStrategy());

//    @Test
//    void javaBaseDockerImageRuns() {
//        try (GenericContainer<?> container =
//                     new GenericContainer<>("qubership/qubership-core-base-image:profiler-latest")
//                             .withEnv("PROFILER_ENABLED", "true")
//                             .withCommand("java", "-jar", "/app/testapp.jar")
//                             .withCopyToContainer(MountableFile.forHostPath(TESTAPP_JAR), "/app/testapp.jar")
//                             .withStartupAttempts(1)
//                             .withStartupTimeout(Duration.ofMinutes(1))
//                             .withLogConsumer(new LogToConsolePrinter("[testApp] "))
//                             .withStartupCheckStrategy(new OneShotStartupCheckStrategy())
//        ) {
//            container.start();
//            // We don't assert the generated profiling output yet, however, we do verify the process starts successfully
//            String stderr = container.getLogs(OutputFrame.OutputType.STDERR);
//            if (!stderr.contains("-javaagent:")) {
//                fail("Container stderr should mention -javaagent: property");
//            }
//        }
//    }

    @Test
    void profilerSendsDataToMockCollector() {
        // Verify profiler agent was enabled
        String profilerLogs = profilerApp.getLogs(OutputFrame.OutputType.STDERR);
        assertTrue(
                profilerLogs.contains("-javaagent:"),
                "Profiler agent should be enabled");

        // Verify mock collector received connection
        assertTrue(
                mockCollector.hasClientConnected(),
                "Mock collector should have received a connection from the profiler");

        // Verify at least one stream was initialized
        assertTrue(
                mockCollector.hasInitializedStream("trace") ||
                        mockCollector.hasInitializedStream("calls") ||
                        mockCollector.hasInitializedStream("xml") ||
                        mockCollector.hasInitializedStream("sql"),
                "At least one profiler stream should be initialized");

        // Optional: verify data was actually sent (may not always happen in quick tests)
        String collectorLogs = mockCollector.getLogs();
        System.out.println("=== Mock Collector Summary ===");
        System.out.println("Client connected: " + mockCollector.hasClientConnected());
        System.out.println("Trace stream initialized: " + mockCollector.hasInitializedStream("trace"));
        System.out.println("==============================");
    }
}
