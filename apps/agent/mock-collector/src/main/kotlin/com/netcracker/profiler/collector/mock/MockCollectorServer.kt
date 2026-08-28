package com.netcracker.profiler.collector.mock

import com.netcracker.profiler.cloud.transport.ProtocolConst
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.slf4j.LoggerFactory
import java.net.ServerSocket
import java.net.Socket
import java.time.Duration
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

/**
 * Mock collector server that accepts connections from Dumper clients
 * and logs received profiling data.
 */
class MockCollectorServer(
    private val bindPort: Int = ProtocolConst.PLAIN_SOCKET_PORT,
    private val backlog: Int = ProtocolConst.PLAIN_SOCKET_BACKLOG,
) : AutoCloseable {
    enum class ServerState {
        IDLE, RUNNING, CLOSING
    }

    val metricRegistry = SimpleMeterRegistry()

    val mockConnections = metricRegistry.counter("mock.server.connections")

    private val log = LoggerFactory.getLogger(MockCollectorServer::class.java)
    private val state = AtomicReference<ServerState>(ServerState.IDLE)
    private var serverSocket: ServerSocket? = null
    private val startupLatch = CountDownLatch(1)
    private val executor: ExecutorService = Executors.newCachedThreadPool { runnable ->
        Thread(runnable, "profiler-mock-collector-${threadCounter.incrementAndGet()}")
    }
    private val activeConnections = ConcurrentHashMap<String, ClientConnectionHandler>()

    companion object {
        private val threadCounter = AtomicInteger(0)
    }

    val port: Int
        get() = serverSocket?.localPort ?: throw IllegalStateException("Server is not running")

    fun started(duration: Duration): MockCollectorServer {
        println("duration = ${duration}")
        start()
        startupLatch.await(duration.toMillis(), TimeUnit.MILLISECONDS)
        println("awat duration = ${duration}")
        return this
    }

    /**
     * Start the server and listen for incoming connections.
     */
    fun start() {
        if (!state.compareAndSet(ServerState.IDLE, ServerState.RUNNING)) {
            log.warn("Can't start since its current state is {}", state)
            return
        }

        executor.submit {
            try {
                serverSocket = ServerSocket(bindPort, backlog)
                startupLatch.countDown()
                log.info("Mock Collector Server started on port {}", port)
                log.info("Waiting for connections from Dumper clients...")

                while (state.get() == ServerState.RUNNING) {
                    try {
                        val clientSocket = serverSocket?.accept() ?: break
                        mockConnections.increment()
                        handleClientConnection(clientSocket)
                    } catch (e: Exception) {
                        if (state.get() == ServerState.RUNNING) {
                            log.error("Error accepting client connection", e)
                        }
                    }
                }
                log.info("Server completed, state={}", state.get())
            } catch (e: Exception) {
                log.error("Failed to start server on port {}", port, e)
                throw e
            } finally {
                serverSocket?.close()
            }
        }
    }

    /**
     * Handle a new client connection.
     */
    private fun handleClientConnection(socket: Socket) {
        val clientAddress = "${socket.inetAddress.hostAddress}:${socket.port}"
        log.info("New connection from {}", clientAddress)

        try {
            val handler = ClientConnectionHandler(socket, this)
            activeConnections[clientAddress] = handler

            executor.submit {
                try {
                    handler.handle()
                } catch (e: Exception) {
                    log.error("Error handling connection from {}", clientAddress, e)
                } finally {
                    activeConnections.remove(clientAddress)
                    log.info("Connection closed: {}", clientAddress)
                }
            }
        } catch (e: Exception) {
            log.error("Failed to create handler for connection from {}", clientAddress, e)
            socket.close()
        }
    }

    /**
     * Stop the server gracefully.
     */
    override fun close() {
        if (!state.compareAndSet(ServerState.RUNNING, ServerState.CLOSING)) {
            log.warn("Can't stop {} server", state.get())
            return
        }

        log.info("Stopping Mock Collector Server...")

        // Close all active connections
        activeConnections.values.forEach { handler ->
            try {
                handler.close()
            } catch (e: Exception) {
                log.error("Error closing connection handler", e)
            }
        }
        activeConnections.clear()

        // Close server socket
        serverSocket?.close()
        serverSocket = null

        // Shutdown executor
        executor.shutdown()
        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                executor.shutdownNow()
            }
        } catch (e: InterruptedException) {
            executor.shutdownNow()
            Thread.currentThread().interrupt()
        }

        log.info("Mock Collector Server stopped")
    }
}
