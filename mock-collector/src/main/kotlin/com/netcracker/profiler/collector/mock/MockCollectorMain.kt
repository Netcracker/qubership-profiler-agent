package com.netcracker.profiler.collector.mock

import com.netcracker.profiler.cloud.transport.ProtocolConst
import org.slf4j.LoggerFactory
import kotlin.system.exitProcess

/**
 * Main entry point for the Mock Collector server.
 *
 * Usage:
 *   ./gradlew :mock-collector:run
 *   ./gradlew :mock-collector:run --args="--port 1715"
 *
 * Arguments:
 *   --port <port>     Port to listen on (default: 1715)
 *   --help            Show this help message
 */
fun main(args: Array<String>) {
    val log = LoggerFactory.getLogger("MockCollectorMain")

    // Parse command line arguments
    var port = ProtocolConst.PLAIN_SOCKET_PORT
    var showHelp = false

    var i = 0
    while (i < args.size) {
        when (args[i]) {
            "--port" -> {
                if (i + 1 < args.size) {
                    port = args[i + 1].toIntOrNull() ?: run {
                        log.error("Invalid port number: {}", args[i + 1])
                        exitProcess(1)
                    }
                    i++
                } else {
                    log.error("--port requires an argument")
                    exitProcess(1)
                }
            }
            "--help", "-h" -> showHelp = true
            else -> {
                log.error("Unknown argument: {}", args[i])
                showHelp = true
            }
        }
        i++
    }

    if (showHelp) {
        printHelp()
        exitProcess(if (args.any { it == "--help" || it == "-h" }) 0 else 1)
    }

    // Print banner
    printBanner()

    log.info("Starting Mock Collector Server...")
    log.info("  Port: {}", port)
    log.info("  Protocol Version: {}", ProtocolConst.PROTOCOL_VERSION_V3)

    // Create and start server
    val server = MockCollectorServer(port = port)

    // Setup shutdown hook
    Runtime.getRuntime().addShutdownHook(Thread {
        log.info("Shutdown signal received")
        server.stop()
    })

    try {
        server.start()
    } catch (e: Exception) {
        log.error("Failed to start server", e)
        exitProcess(1)
    }
}

private fun printBanner() {
    val banner = """
        ╔════════════════════════════════════════════════════════════════╗
        ║                                                                ║
        ║              Mock Profiler Collector Server                    ║
        ║              Qubership Profiler Agent                          ║
        ║                                                                ║
        ║  This mock collector receives and logs profiling data          ║
        ║  sent from Dumper instances for testing and debugging.         ║
        ║                                                                ║
        ╚════════════════════════════════════════════════════════════════╝
    """.trimIndent()

    println(banner)
}

private fun printHelp() {
    val help = """
        Mock Profiler Collector Server

        Usage:
          ./gradlew :mock-collector:run [OPTIONS]

        Options:
          --port <port>     Port to listen on (default: ${ProtocolConst.PLAIN_SOCKET_PORT})
          --help, -h        Show this help message

        Examples:
          # Run with default port (1715)
          ./gradlew :mock-collector:run

          # Run on custom port
          ./gradlew :mock-collector:run --args="--port 8080"

        Configuration:
          The Dumper client can be configured to connect to this mock collector
          using environment variables or system properties:

            REMOTE_DUMP_HOST=localhost
            REMOTE_DUMP_PORT_PLAIN=${ProtocolConst.PLAIN_SOCKET_PORT}

          Or for the profiled application:

            java -javaagent:profiler-agent.jar \
                 -DREMOTE_DUMP_HOST=localhost \
                 -DREMOTE_DUMP_PORT_PLAIN=${ProtocolConst.PLAIN_SOCKET_PORT} \
                 -jar your-application.jar
    """.trimIndent()

    println(help)
}
