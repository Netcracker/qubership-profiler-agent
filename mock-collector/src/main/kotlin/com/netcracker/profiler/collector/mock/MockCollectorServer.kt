package com.netcracker.profiler.collector.mock

import com.netcracker.profiler.cloud.transport.ProtocolConst
import org.slf4j.LoggerFactory
import java.net.ServerSocket
import java.net.Socket
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

/**
 * Mock collector server that accepts connections from Dumper clients
 * and logs received profiling data.
 */
class MockCollectorServer(
    private val port: Int = ProtocolConst.PLAIN_SOCKET_PORT,
    private val backlog: Int = ProtocolConst.PLAIN_SOCKET_BACKLOG
) {
    private val log = LoggerFactory.getLogger(MockCollectorServer::class.java)
    private val running = AtomicBoolean(false)
    private var serverSocket: ServerSocket? = null
    private val executor: ExecutorService = Executors.newCachedThreadPool { runnable ->
        Thread(runnable, "collector-handler-${threadCounter.incrementAndGet()}")
    }
    private val activeConnections = ConcurrentHashMap<String, ClientConnectionHandler>()

    companion object {
        private val threadCounter = AtomicInteger(0)
    }

    /**
     * Start the server and listen for incoming connections.
     */
    fun start() {
        if (running.getAndSet(true)) {
            log.warn("Server is already running")
            return
        }

        try {
            serverSocket = ServerSocket(port, backlog)
            log.info("Mock Collector Server started on port {}", port)
            log.info("Waiting for connections from Dumper clients...")

            while (running.get()) {
                try {
                    val clientSocket = serverSocket?.accept() ?: break
                    handleClientConnection(clientSocket)
                } catch (e: Exception) {
                    if (running.get()) {
                        log.error("Error accepting client connection", e)
                    }
                }
            }
        } catch (e: Exception) {
            log.error("Failed to start server on port {}", port, e)
            throw e
        } finally {
            cleanup()
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
    fun stop() {
        if (!running.getAndSet(false)) {
            log.warn("Server is not running")
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

    private fun cleanup() {
        running.set(false)
        serverSocket?.close()
        serverSocket = null
    }
}
