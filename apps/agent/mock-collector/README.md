# Mock Profiler Collector

A Java library that provides a mock collector server for receiving and logging profiling data sent from Dumper instances. This module is useful for testing, debugging, and understanding the profiler data collection protocol.

## Features

- **Java API**: Programmatic server control with `MockCollectorServer` class
- **Protocol Support**: Implements the Qubership Profiler protocol (versions V2 and V3)
- **Real-time Logging**: Logs received data with detailed information about streams, sources, and content
- **Stream Management**: Tracks multiple concurrent data streams (trace, calls, sql, xml, etc.)
- **Metrics**: Provides Micrometer metrics registry for monitoring connections and data
- **AutoCloseable**: Try-with-resources support for easy cleanup
- **Debugging**: Shows data previews with hex dump and text detection

## Installation

Add the dependency to your test configuration:

```kotlin
// In build.gradle.kts
dependencies {
    testImplementation(project(":mock-collector"))
}
```

## Usage

### Basic Usage

```java
import com.netcracker.profiler.collector.mock.MockCollectorServer;
import java.time.Duration;

// Create and start the server
try (MockCollectorServer server = new MockCollectorServer()) {
    server.started(Duration.ofSeconds(10));

    int port = server.port;
    System.out.println("Mock collector listening on port: " + port);

    // Run your tests here...
    // The profiler agent should connect to localhost:1715

    // Server automatically closes when exiting try-with-resources
}
```

### Custom Port

```java
// Use a specific port
MockCollectorServer server = new MockCollectorServer(8080, 50);
server.start();

// Or use port 0 for a random available port
MockCollectorServer server = new MockCollectorServer(0, 50);
server.start();
int actualPort = server.port;  // Get the assigned port
```

### Accessing Metrics

```java
MockCollectorServer server = new MockCollectorServer();
server.start();

// Check connection count
double connectionCount = server.mockConnections.count();

// Access the full metrics registry
MeterRegistry registry = server.metricRegistry;
```

### Integration with Testcontainers

```java
// Start mock server on host machine
try (MockCollectorServer mockServer = new MockCollectorServer()) {
    mockServer.started(Duration.ofSeconds(10));

    // Determine host address accessible from container
    String hostForContainer = System.getProperty("os.name")
        .toLowerCase().contains("mac") ? "host.docker.internal" : "172.17.0.1";

    // Start profiled app container
    try (GenericContainer<?> profilerApp = new GenericContainer<>("java-app:latest")
        .withEnv("REMOTE_DUMP_HOST", hostForContainer)
        .withEnv("REMOTE_DUMP_PORT_PLAIN", String.valueOf(mockServer.port))) {

        profilerApp.start();

        // Wait for data transmission
        Thread.sleep(2000);

        // Verify connection
        assert mockServer.mockConnections.count() > 0;
    }
}
```

## API Reference

### MockCollectorServer

Main server class that accepts TCP connections from profiler agents.

**Constructor:**
```java
MockCollectorServer()  // Uses default port 1715 and backlog 50
MockCollectorServer(int bindPort, int backlog)
```

**Methods:**
- `void start()` - Start the server asynchronously
- `MockCollectorServer started(Duration timeout)` - Start and wait for server to be ready
- `int getPort()` - Get the actual port the server is listening on
- `void close()` - Gracefully shutdown the server and all connections

**Properties:**
- `MeterRegistry metricRegistry` - Micrometer metrics registry
- `Counter mockConnections` - Connection counter metric
- `ServerState state` - Current server state (IDLE, RUNNING, CLOSING)

### Internal Components

#### ClientConnectionHandler
- Handles the protocol handshake (version negotiation)
- Processes commands from the Dumper client:
  - `COMMAND_INIT_STREAM_V2` - Initialize a data stream
  - `COMMAND_RCV_DATA` - Receive data chunk
  - `COMMAND_REQUEST_ACK_FLUSH` - Flush acknowledgments
  - `COMMAND_CLOSE` - Close connection
- Sends ACK responses back to the client

#### StreamManager
- Tracks active data streams by UUID handle
- Maintains statistics for each stream
- Maps stream names (trace, calls, xml, sql, etc.) to handles

#### DataLogger
- Logs received data with formatting
- Provides data previews (hex dump or text)
- Detects text vs binary data automatically
- Tracks total data received

## Protocol Overview

The Profiler protocol is a custom binary protocol:

1. **Handshake**:
   - Client sends `COMMAND_GET_PROTOCOL_VERSION_V2`
   - Client sends protocol version, pod name, microservice name, namespace
   - Server responds with supported protocol version

2. **Stream Initialization**:
   - Client sends `COMMAND_INIT_STREAM_V2` with stream name
   - Server creates UUID handle and sends back stream configuration

3. **Data Transfer**:
   - Client sends `COMMAND_RCV_DATA` with stream handle and data
   - Server logs the data and sends ACK response

4. **Graceful Shutdown**:
   - Client sends `COMMAND_CLOSE`
   - Connection is closed

## Logging

The server uses SLF4J for logging. Configure logging in your test framework:

```xml
<!-- Example logback-test.xml -->
<configuration>
    <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
            <pattern>%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n</pattern>
        </encoder>
    </appender>

    <logger name="com.netcracker.profiler.collector.mock" level="INFO"/>

    <root level="INFO">
        <appender-ref ref="STDOUT"/>
    </root>
</configuration>
```

## Example Output

```none
================================================================================
Data Chunk Received #1
--------------------------------------------------------------------------------
  Timestamp:       2025-10-13 18:30:45.123
  Stream:          trace
  Source:          default/my-app/pod-12345
  Size:            2048 bytes (2.00 KB)
  Total Received:  2048 bytes (0.00 MB in 1 chunks)
  Data Preview:
    [Hex dump - first 256 bytes]
    0000: 00 00 00 00 00 00 00 01 00 00 00 00 00 00 00 02  | ................
    0010: 48 65 6C 6C 6F 20 57 6F 72 6C 64 0A 00 00 00 03  | Hello World.....
    ...
================================================================================
```

## Use Cases

- **Integration Testing**: Verify that the profiler agent sends data correctly
- **Protocol Debugging**: Inspect the actual binary protocol data exchanged
- **Development**: Test changes to the Dumper or protocol implementation
- **CI/CD**: Automated testing of profiler agent in containerized environments
- **Education**: Learn how the profiler protocol works

## Best Practices

1. **Use try-with-resources**: Always use try-with-resources to ensure proper cleanup
   ```java
   try (MockCollectorServer server = new MockCollectorServer()) {
       // Your test code
   }
   ```

2. **Random ports for parallel tests**: Use port 0 to avoid port conflicts
   ```java
   MockCollectorServer server = new MockCollectorServer(0, 50);
   ```

3. **Wait for startup**: Always use `started(Duration)` to ensure server is ready
   ```java
   server.started(Duration.ofSeconds(10));
   ```

4. **Check metrics**: Use the metrics registry to verify connections
   ```java
   assertEquals(1.0, server.mockConnections.count());
   ```

## Limitations

- This is a mock/test implementation - not production-ready
- Does not persist data to files (only logs it)
- Does not implement all collector features (e.g., remote commands)
- No SSL/TLS support yet (plain socket only)
- Designed for testing environments, not high-throughput scenarios

## Documentation

For integration test examples, see [INTEGRATION.md](INTEGRATION.md).

## Future Enhancements

Potential improvements:

- [ ] Add SSL/TLS support
- [ ] Optionally save received data to files
- [ ] Stream filtering and search capabilities
- [ ] Additional Micrometer metrics (bytes received, stream counts, etc.)
- [ ] Support for protocol version negotiation testing
