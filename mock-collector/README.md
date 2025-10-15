# Mock Profiler Collector

A mock collector server that receives and logs profiling data sent from Dumper instances. This module is useful for testing, debugging, and understanding the profiler data collection protocol.

## Features

- **Protocol Support**: Implements the Qubership Profiler protocol (versions V2 and V3)
- **Real-time Logging**: Logs received data with detailed information about streams, sources, and content
- **Stream Management**: Tracks multiple concurrent data streams (trace, calls, sql, xml, etc.)
- **Statistics**: Provides detailed statistics about received data
- **Configurable**: Easy configuration via command-line arguments
- **Debugging**: Shows data previews with hex dump and text detection

## Building

Build the module using Gradle:

```bash
./gradlew :mock-collector:build
```

## Running

### Using Gradle

Run directly with Gradle:

```bash
./gradlew :mock-collector:run
```

Or on a custom port:

```bash
./gradlew :mock-collector:run --args="--port 8080"
```

### Using the Distribution

Build a distribution package:

```bash
./gradlew :mock-collector:installDist
```

Then run the generated script:

```bash
./mock-collector/build/install/mock-collector/bin/mock-collector
```

## Command Line Arguments

- `--port <port>` - Port to listen on (default: 1715)
- `--help, -h` - Show help message

## Configuring the Profiler Agent

To make the Profiler Agent send data to this mock collector, configure it with:

### Environment Variables

```bash
export REMOTE_DUMP_HOST=localhost
export REMOTE_DUMP_PORT_PLAIN=1715
```

### System Properties

```bash
java -javaagent:profiler-agent.jar \
     -DREMOTE_DUMP_HOST=localhost \
     -DREMOTE_DUMP_PORT_PLAIN=1715 \
     -jar your-application.jar
```

### Disabling Local Dump

If you want to send data ONLY to the remote collector (and not write local files), set:

```bash
export FORCE_LOCAL_DUMP=false
```

Or:

```bash
java -DFORCE_LOCAL_DUMP=false -javaagent:...
```

## Architecture

The mock collector consists of several components:

### MockCollectorServer
- Main server that accepts TCP connections
- Listens on port 1715 (or custom port)
- Spawns a handler thread for each client connection

### ClientConnectionHandler
- Handles the protocol handshake (version negotiation)
- Processes commands from the Dumper client:
  - `COMMAND_INIT_STREAM_V2` - Initialize a data stream
  - `COMMAND_RCV_DATA` - Receive data chunk
  - `COMMAND_REQUEST_ACK_FLUSH` - Flush acknowledgments
  - `COMMAND_CLOSE` - Close connection
- Sends ACK responses back to the client

### StreamManager
- Tracks active data streams by UUID handle
- Maintains statistics for each stream
- Maps stream names (trace, calls, xml, sql, etc.) to handles

### DataLogger
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

Logs are written to:
- Console (INFO level)
- `logs/mock-collector.log` (DEBUG level)

Configure logging by editing `src/main/resources/logback.xml`.

## Example Output

```
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

- **Testing**: Verify that the Dumper is sending data correctly
- **Debugging**: Inspect the actual binary protocol data
- **Development**: Test changes to the Dumper or protocol
- **Education**: Learn how the profiler protocol works
- **Integration**: Develop alternative collector implementations

## Limitations

- This is a mock/test implementation - not production-ready
- Does not persist data to files (only logs it)
- Does not implement all collector features (e.g., remote commands)
- No SSL/TLS support yet (plain socket only)

## Future Enhancements

Potential improvements:

- [ ] Add SSL/TLS support
- [ ] Optionally save received data to files
- [ ] Web UI to view received data
- [ ] Stream filtering and search
- [ ] Prometheus metrics export
- [ ] Docker container support
