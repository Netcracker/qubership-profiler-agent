package com.netcracker.profiler.collector.mock

import org.slf4j.LoggerFactory
import java.io.Closeable
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * Manages active data streams from clients.
 * Tracks stream metadata and statistics.
 */
class StreamManager : Closeable {
    private val log = LoggerFactory.getLogger(StreamManager::class.java)
    private val streams = ConcurrentHashMap<UUID, StreamInfo>()

    /**
     * Register a new stream.
     */
    fun registerStream(handle: UUID, name: String, rollingSequenceId: Int) {
        val streamInfo = StreamInfo(
            handle = handle,
            name = name,
            rollingSequenceId = rollingSequenceId,
            createdAt = System.currentTimeMillis()
        )
        streams[handle] = streamInfo
        log.info("Stream registered: {} (name={}, rollingSeqId={})", handle, name, rollingSequenceId)
    }

    /**
     * Get stream information by handle.
     */
    fun getStream(handle: UUID): StreamInfo? {
        return streams[handle]
    }

    /**
     * Record received data for a stream.
     */
    fun recordData(handle: UUID, size: Int) {
        val stream = streams[handle]
        if (stream != null) {
            stream.bytesReceived.addAndGet(size.toLong())
            stream.chunksReceived.incrementAndGet()
            stream.lastActivityAt = System.currentTimeMillis()
        }
    }

    /**
     * Get all active streams.
     */
    fun getAllStreams(): Collection<StreamInfo> {
        return streams.values
    }

    /**
     * Print statistics for all streams.
     */
    fun printStatistics() {
        if (streams.isEmpty()) {
            log.info("No active streams")
            return
        }

        log.info("=== Stream Statistics ===")
        streams.values.forEach { stream ->
            val duration = System.currentTimeMillis() - stream.createdAt
            val bytesReceived = stream.bytesReceived.get()
            val chunksReceived = stream.chunksReceived.get()

            log.info("  Stream: {} ({})", stream.name, stream.handle)
            log.info("    Rolling Sequence ID: {}", stream.rollingSequenceId)
            log.info("    Bytes Received: {} ({} KB)", bytesReceived, bytesReceived / 1024)
            log.info("    Chunks Received: {}", chunksReceived)
            log.info("    Duration: {} ms", duration)
            log.info("    Avg Chunk Size: {} bytes", if (chunksReceived > 0) bytesReceived / chunksReceived else 0)
        }
        log.info("=========================")
    }

    override fun close() {
        printStatistics()
        streams.clear()
    }

    /**
     * Information about an active stream.
     */
    data class StreamInfo(
        val handle: UUID,
        val name: String,
        val rollingSequenceId: Int,
        val createdAt: Long,
        var lastActivityAt: Long = createdAt,
        val bytesReceived: AtomicLong = AtomicLong(0),
        val chunksReceived: AtomicLong = AtomicLong(0)
    )
}
