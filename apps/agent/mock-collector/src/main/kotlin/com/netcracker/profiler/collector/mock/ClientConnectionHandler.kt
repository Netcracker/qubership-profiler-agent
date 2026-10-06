package com.netcracker.profiler.collector.mock

import com.netcracker.profiler.cloud.transport.EndlessSocketInputStream
import com.netcracker.profiler.cloud.transport.FieldIO
import com.netcracker.profiler.cloud.transport.ProfilerProtocolException
import com.netcracker.profiler.cloud.transport.ProtocolConst
import org.slf4j.LoggerFactory
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.Closeable
import java.net.Socket
import java.net.SocketTimeoutException
import java.util.*

/**
 * Handles a single client connection from a Dumper instance.
 * Implements the collector protocol to receive and log profiling data.
 */
class ClientConnectionHandler(
    private val socket: Socket,
    private val server: MockCollectorServer
) : Closeable {
    private val log = LoggerFactory.getLogger(ClientConnectionHandler::class.java)
    private val streamManager = StreamManager()
    private val dataLogger = DataLogger()

    private lateinit var fieldIO: FieldIO
    private lateinit var inputStream: BufferedInputStream
    private lateinit var outputStream: BufferedOutputStream

    private var podName: String = "unknown"
    private var microserviceName: String = "unknown"
    private var cloudNamespace: String = "unknown"
    private var protocolVersion: Long = -1

    /**
     * Main handler loop for processing client commands.
     */
    fun handle() {
        try {
            initializeConnection()
            processCommands()
        } catch (e: SocketTimeoutException) {
            log.debug("Socket timeout for {}:{}", microserviceName, podName)
        } catch (e: ProfilerProtocolException) {
            log.error("Protocol error for {}:{}: {}", microserviceName, podName, e.message)
        } catch (e: Exception) {
            log.error("Error handling connection from {}:{}", microserviceName, podName, e)
        } finally {
            close()
        }
    }

    /**
     * Initialize connection and perform protocol handshake.
     */
    private fun initializeConnection() {
        socket.soTimeout = ProtocolConst.PLAIN_SOCKET_READ_TIMEOUT
        socket.sendBufferSize = ProtocolConst.PLAIN_SOCKET_SND_BUFFER_SIZE
        socket.receiveBufferSize = ProtocolConst.PLAIN_SOCKET_RCV_BUFFER_SIZE
        socket.keepAlive = true

        outputStream = BufferedOutputStream(socket.getOutputStream(), ProtocolConst.DATA_BUFFER_SIZE)
        val socketInput = socket.getInputStream()
        inputStream = BufferedInputStream(EndlessSocketInputStream(socketInput), ProtocolConst.DATA_BUFFER_SIZE)

        fieldIO = FieldIO(socket, inputStream, outputStream)

        performHandshake()
    }

    /**
     * Perform protocol version negotiation and client identification.
     */
    private fun performHandshake() {
        // Read handshake command
        val command = inputStream.read()
        if (command != ProtocolConst.COMMAND_GET_PROTOCOL_VERSION_V2) {
            throw ProfilerProtocolException("Expected COMMAND_GET_PROTOCOL_VERSION_V2 but got $command")
        }

        // Read client protocol version
        val clientVersion = fieldIO.readLong()

        // Read client identification
        podName = fieldIO.readString()
        microserviceName = fieldIO.readString()
        cloudNamespace = fieldIO.readString()

        log.info(
            "Client handshake: pod={}, microservice={}, namespace={}, version={}",
            podName, microserviceName, cloudNamespace, clientVersion
        )

        // Determine protocol version to use
        protocolVersion = when {
            clientVersion >= ProtocolConst.PROTOCOL_VERSION_V3 -> ProtocolConst.PROTOCOL_VERSION_V3
            clientVersion >= ProtocolConst.PROTOCOL_VERSION_V2 -> ProtocolConst.PROTOCOL_VERSION_V2
            else -> throw ProfilerProtocolException("Unsupported client protocol version: $clientVersion")
        }

        // Send protocol version response
        fieldIO.writeLong(protocolVersion)
        outputStream.flush()

        log.info("Handshake completed with protocol version {}", protocolVersion)
    }

    /**
     * Main command processing loop.
     */
    private fun processCommands() {
        while (!socket.isClosed && socket.isConnected) {
            try {
                val command = inputStream.read()

                if (command < 0) {
                    log.debug("End of stream from {}:{}", microserviceName, podName)
                    break
                }

                when (command) {
                    ProtocolConst.COMMAND_INIT_STREAM_V2 -> handleInitStream()
                    ProtocolConst.COMMAND_RCV_DATA -> handleReceiveData()
                    ProtocolConst.COMMAND_REQUEST_ACK_FLUSH -> handleAckFlush()
                    ProtocolConst.COMMAND_CLOSE -> {
                        log.info("Client {}:{} requested close", microserviceName, podName)
                        break
                    }

                    ProtocolConst.COMMAND_KEEP_ALIVE -> handleKeepAlive()
                    else -> {
                        log.warn("Unknown command: {} from {}:{}", command, microserviceName, podName)
                    }
                }
            } catch (e: SocketTimeoutException) {
                // Normal timeout, continue
                continue
            } catch (e: Exception) {
                log.error("Error processing command from {}:{}", microserviceName, podName, e)
                break
            }
        }
    }

    /**
     * Handle COMMAND_INIT_STREAM_V2 - initialize a new data stream.
     */
    private fun handleInitStream() {
        val streamName = fieldIO.readString()
        val requestedRollingSequenceId = fieldIO.readInt()
        val resetRequired = fieldIO.readInt() != 0

        server.metricRegistry.counter("mock.server.streams", "stream_name", streamName).increment()

        log.info(
            "Initializing stream: name={}, rollingSeqId={}, reset={}",
            streamName, requestedRollingSequenceId, resetRequired
        )

        // Create stream handle
        val streamHandle = UUID.randomUUID()
        streamManager.registerStream(streamHandle, streamName, requestedRollingSequenceId)

        // Send response
        fieldIO.writeUUID(streamHandle)
        fieldIO.writeLong(0L) // rotation period (not used in mock)
        fieldIO.writeLong(0L) // required rotation size (not used in mock)
        fieldIO.writeInt(requestedRollingSequenceId) // server rolling sequence id
        outputStream.flush()

        log.debug("Stream initialized: {} -> {}", streamName, streamHandle)
    }

    /**
     * Handle COMMAND_RCV_DATA - receive data chunk.
     */
    private fun handleReceiveData() {
        val streamHandle = fieldIO.readUUID()
        val dataLength = fieldIO.readField()

        if (streamHandle == null) {
            log.warn("Received data for null stream handle")
            sendAck(1) // Send error ack
            return
        }

        val streamInfo = streamManager.getStream(streamHandle)
        if (streamInfo == null) {
            log.warn("Received data for unknown stream: {}", streamHandle)
            sendAck(1) // Send error ack
            return
        }

        server.metricRegistry.counter("mock.server.stream.chunks", "stream_name", streamInfo.name).increment()
        server.metricRegistry.counter("mock.server.stream.bytes", "stream_name", streamInfo.name)
            .increment(dataLength.toDouble())

        // Get data from field IO buffer
        val data = fieldIO.array.copyOf(dataLength)

        // Log received data
        dataLogger.logData(
            streamInfo.name, data, dataLength,
            podName, microserviceName, cloudNamespace
        )

        // Update stream statistics
        streamManager.recordData(streamHandle, dataLength)

        // Send success ack (0 commands to dispatch)
        sendAck(0)
    }

    /**
     * Handle COMMAND_REQUEST_ACK_FLUSH - explicit ack request.
     */
    private fun handleAckFlush() {
        log.trace("Ack flush requested by {}:{}", microserviceName, podName)
        sendAck(0)
    }

    /**
     * Handle COMMAND_KEEP_ALIVE - keep connection alive.
     */
    private fun handleKeepAlive() {
        log.trace("Keep-alive from {}:{}", microserviceName, podName)
    }

    /**
     * Send acknowledgment back to client.
     *
     * @param numCommands Number of commands to dispatch (0 for normal ack)
     */
    private fun sendAck(numCommands: Int) {
        outputStream.write(numCommands)
        outputStream.flush()
    }

    override fun close() {
        try {
            log.info("Closing connection from {}:{}", microserviceName, podName)
            streamManager.close()
            socket.close()
        } catch (e: Exception) {
            log.error("Error closing connection", e)
        }
    }
}
