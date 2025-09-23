package com.netcracker.profiler.test.agent;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.startupcheck.OneShotStartupCheckStrategy;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

import java.time.Duration;

@Testcontainers
public class JavaBaseImageTest {
    private static final String BASE_IMAGE_BUILD_ROOT = System.getProperty("qubership.profiler.test.java-base.root");
    private static final String TESTAPP_JAR = System.getProperty("qubership.profiler.testapp.jar");

    static {
        assertNotNull(BASE_IMAGE_BUILD_ROOT, "qubership.profiler.test.java-base.root system property must be set");
    }

    @Test
    void javaBaseDockerImageRuns() {
        try (GenericContainer<?> container =
                     new GenericContainer<>("qubership/qubership-core-base-image:profiler-latest")
                             .withEnv("PROFILER_ENABLED", "true")
                             .withCommand("java", "-jar", "/app/testapp.jar")
                             .withCopyToContainer(MountableFile.forHostPath(TESTAPP_JAR), "/app/testapp.jar")
                             .withStartupAttempts(1)
                             .withStartupTimeout(Duration.ofMinutes(1))
                             .withLogConsumer(new LogToConsolePrinter("[testApp] "))
                             .withStartupCheckStrategy(new OneShotStartupCheckStrategy())
        ) {
            container.start();
            // We don't assert the generated profiling output yet, however, we do verify the process starts successfully
            String stderr = container.getLogs(OutputFrame.OutputType.STDERR);
            if (!stderr.contains("-javaagent:")) {
                fail("Container stderr should mention -javaagent: property");
            }
        }
    }
}
