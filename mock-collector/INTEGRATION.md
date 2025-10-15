# Mock Collector Integration with Tests

This document describes how the mock-collector module is integrated with the profiler agent integration tests.

## Architecture

The integration test setup uses Testcontainers to spin up two Docker containers that communicate over a shared network:

```
┌─────────────────────────────────────────────────────────┐
│  JavaBaseImageTest (JUnit + Testcontainers)            │
│  - Creates shared Docker network                        │
│  - Manages container lifecycle                          │
│  - Performs assertions on logs                          │
└────────┬──────────────────────────────┬─────────────────┘
         │                              │
         │ spawns                       │ spawns
         ▼                              ▼
┌────────────────────────┐    ┌──────────────────────────┐
│  Mock Collector        │◄───│  Profiled Application    │
│  Container             │    │  Container               │
│                        │    │                          │
│  - Port: 1715          │    │  - java-base-image       │
│  - Network: test-net   │    │  - Profiler Agent        │
│  - Alias: mock-collec. │    │  - Test Application      │
└────────────────────────┘    └──────────────────────────┘
         │                              │
         └──────────────┬───────────────┘
                  Testcontainers Network
                  (mock-collector:1715)
```

## Components

### 1. Dockerfile (`mock-collector/Dockerfile`)

Defines the Docker image for the mock collector:

- **Base Image**: `eclipse-temurin:17-jre-alpine` (lightweight JRE)
- **Contents**: Mock collector distribution from `build/install/mock-collector`
- **Port**: Exposes 1715 (default profiler port)
- **Entry Point**: Runs `mock-collector` binary

### 2. Docker Build Task (`mock-collector/build.gradle.kts`)

Gradle task that builds the Docker image:

```kotlin
val buildMockCollectorDockerImage by tasks.registering(Exec::class) {
    description = "Builds Docker image for mock-collector (for integration tests)"
    dependsOn(tasks.installDist)

    executable = "docker"
    args("build", "-t", "qubership/mock-collector:test", ...)
}
```

### 3. MockCollectorContainer (`installer/src/test/java/.../MockCollectorContainer.java`)

Testcontainers wrapper that provides:

**Configuration:**
- Pre-configured with port 1715
- Wait strategy for "Mock Collector Server started" log message
- Log forwarding with `[MockCollector]` prefix

**Helper Methods:**
- `hasClientConnected()` - Check if a client connected
- `hasInitializedStream(streamName)` - Check if stream was initialized
- `hasReceivedData()` - Check if any data was received
- `getReceivedChunksCount()` - Count data chunks in logs

### 4. Integration Test (`installer/src/test/java/.../JavaBaseImageTest.java`)

New test method: `profilerSendsDataToMockCollector()`

**Setup:**
```java
// 1. Create shared network
Network network = Network.newNetwork();

// 2. Start mock collector with network alias
MockCollectorContainer mockCollector = new MockCollectorContainer()
    .withNetwork(network)
    .withNetworkAliases("mock-collector");

// 3. Start profiled app pointing to collector
GenericContainer<?> profilerApp = new GenericContainer<>(...)
    .withNetwork(network)
    .withEnv("REMOTE_DUMP_HOST", "mock-collector")
    .withEnv("REMOTE_DUMP_PORT_PLAIN", "1715");
```

**Assertions:**
- Profiler agent is enabled (check `-javaagent:` in logs)
- Mock collector receives connection
- At least one stream is initialized (trace/calls/xml/sql)

## Build Integration

### Dependencies

In `installer/build.gradle.kts`:

```kotlin
val buildMockCollectorImage by tasks.registering {
    dependsOn(":mock-collector:buildMockCollectorDockerImage")
}

tasks.test {
    dependsOn(buildBaseImage, buildMockCollectorImage, testAppJar)
}
```

This ensures:
1. Mock collector Docker image is built before tests run
2. Java base image with profiler agent is built
3. Test application JAR is created

## Running the Tests

### Run all installer tests:
```bash
./gradlew :installer:test
```

### Run only the integration test:
```bash
./gradlew :installer:test --tests "JavaBaseImageTest.profilerSendsDataToMockCollector"
```

### Run with verbose logging:
```bash
./gradlew :installer:test --info
```

## Test Flow

1. **Test Initialization**
   - Gradle builds both Docker images (base-image + mock-collector)
   - JUnit test creates a Testcontainers Network

2. **Container Startup**
   - Mock collector container starts first
   - Waits for "Mock Collector Server started" in logs
   - Network alias "mock-collector" is registered

3. **Profiled App Startup**
   - Java base image container starts with profiler agent
   - Environment variables point to mock-collector
   - Profiler agent connects to `mock-collector:1715`

4. **Data Flow**
   - Profiler agent sends handshake
   - Mock collector accepts connection
   - Streams are initialized (trace, calls, etc.)
   - Profiler sends data chunks
   - Mock collector logs received data

5. **Verification**
   - Test waits 2 seconds for data transmission
   - Parses mock collector logs for assertions
   - Prints summary of received data

6. **Cleanup**
   - Testcontainers automatically stops containers
   - Removes network
   - Cleans up resources

## Log Output Example

When the test runs, you'll see logs like:

```
[MockCollector] ╔════════════════════════════════════════════╗
[MockCollector] ║    Mock Profiler Collector Server          ║
[MockCollector] ╚════════════════════════════════════════════╝
[MockCollector] Mock Collector Server started on port 1715
[MockCollector] New connection from 172.17.0.3:54321
[MockCollector] Client handshake: pod=pod_12345, microservice=testapp, namespace=default
[MockCollector] Initializing stream: name=trace, rollingSeqId=0
[MockCollector] Stream initialized: trace -> a1b2c3d4-...
[MockCollector] Data Chunk Received #1
[MockCollector]   Stream: trace
[MockCollector]   Size: 2048 bytes

[profilerApp] -javaagent:/opt/qubership/profiler/profiler.jar
[profilerApp] Hello World!

=== Mock Collector Summary ===
Client connected: true
Data chunks received: 1
Trace stream initialized: true
==============================
```

## Troubleshooting

### Docker Build Fails

**Problem**: `docker build` command fails

**Solutions**:
- Ensure Docker is running
- Check Docker daemon has sufficient resources
- Try building manually: `docker build -t qubership/mock-collector:test -f mock-collector/Dockerfile mock-collector/`

### Container Connection Timeout

**Problem**: Test fails with timeout waiting for connection

**Solutions**:
- Check both containers are on the same network
- Verify network alias is "mock-collector"
- Ensure port 1715 is not blocked
- Check mock collector logs for startup errors

### No Data Received

**Problem**: Mock collector doesn't receive data

**Solutions**:
- Increase sleep time in test (currently 2 seconds)
- Check profiled app environment variables
- Verify profiler agent is enabled (check logs for `-javaagent:`)
- Ensure `REMOTE_DUMP_HOST` and `REMOTE_DUMP_PORT_PLAIN` are set

### Test Fails in CI/CD

**Problem**: Test passes locally but fails in CI

**Solutions**:
- Ensure Docker is available in CI environment
- Check CI has sufficient memory (containers need ~512MB each)
- Increase timeout durations
- Enable debug logging: `--info` or `--debug`

## Advanced Configuration

### Custom Collector Port

To use a different port:

```java
MockCollectorContainer mockCollector = new MockCollectorContainer()
    .withCommand("--port", "8080")
    .withExposedPorts(8080);

// Update profiled app environment
.withEnv("REMOTE_DUMP_PORT_PLAIN", "8080")
```

### Persistent Collector Logs

To save mock collector logs to a file:

```java
mockCollector.withLogConsumer(new ToFileConsumer(new File("collector.log")));
```

### Network Debugging

To inspect network traffic:

```bash
# List running containers
docker ps

# Inspect network
docker network inspect <network-id>

# Check connectivity from profiler app
docker exec <profiler-container-id> ping mock-collector
```

## Future Enhancements

Potential improvements:

- [ ] Add SSL/TLS test variant
- [ ] Test with multiple simultaneous profiled apps
- [ ] Verify specific protocol commands in logs
- [ ] Add performance benchmarks
- [ ] Test stream rotation scenarios
- [ ] Add negative tests (connection failures, etc.)
