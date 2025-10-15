package com.netcracker.profiler.test.agent;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

/**
 * Testcontainers wrapper for the Mock Profiler Collector.
 * Provides pre-configured setup for integration testing.
 */
public class MockCollectorContainer extends GenericContainer<MockCollectorContainer> {
    private static final String IMAGE_NAME = "qubership/mock-collector:test";
    private static final int COLLECTOR_PORT = 1715;

    public MockCollectorContainer() {
        super(DockerImageName.parse(IMAGE_NAME));

        // Expose the collector port
        withExposedPorts(COLLECTOR_PORT);

        // Wait for the server to start
        waitingFor(Wait.forLogMessage(".*Mock Collector Server started.*", 1)
                .withStartupTimeout(Duration.ofSeconds(30)));

        // Forward logs to console for debugging
        withLogConsumer(new LogToConsolePrinter("[MockCollector] "));
    }

    /**
     * Get the host-accessible URL for the collector.
     * Use this when connecting from the host machine (not from other containers).
     */
    public String getCollectorUrl() {
        return String.format("%s:%d", getHost(), getMappedPort(COLLECTOR_PORT));
    }

    /**
     * Get the internal port that other containers can use to connect.
     * When containers are on the same network, they should connect to this port
     * using the network alias.
     */
    public int getCollectorPort() {
        return COLLECTOR_PORT;
    }

    /**
     * Check if the collector received any data by examining logs.
     */
    public boolean hasReceivedData() {
        String logs = getLogs();
        return logs.contains("Data Chunk Received");
    }

    /**
     * Check if a specific stream was initialized.
     */
    public boolean hasInitializedStream(String streamName) {
        String logs = getLogs();
        return logs.contains("Stream initialized: " + streamName) ||
               logs.contains("Initializing stream: name=" + streamName);
    }

    /**
     * Check if a client connected successfully.
     */
    public boolean hasClientConnected() {
        String logs = getLogs();
        return logs.contains("Client handshake:") || logs.contains("New connection from");
    }
}
