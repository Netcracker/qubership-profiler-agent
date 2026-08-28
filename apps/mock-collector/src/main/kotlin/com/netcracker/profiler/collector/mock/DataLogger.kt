package com.netcracker.profiler.collector.mock

import org.slf4j.LoggerFactory
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.concurrent.atomic.AtomicLong

/**
 * Logs received profiling data with formatting and statistics.
 */
class DataLogger {
    private val log = LoggerFactory.getLogger(DataLogger::class.java)
    private val totalBytesReceived = AtomicLong(0)
    private val totalChunksReceived = AtomicLong(0)

    private val timeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
        .withZone(ZoneId.systemDefault())

    /**
     * Log received data chunk.
     */
    fun logData(
        streamName: String,
        data: ByteArray,
        length: Int,
        podName: String,
        microserviceName: String,
        cloudNamespace: String
    ) {
        totalBytesReceived.addAndGet(length.toLong())
        val chunkNumber = totalChunksReceived.incrementAndGet()

        val timestamp = timeFormatter.format(Instant.now())

        log.info("=".repeat(80))
        log.info("Data Chunk Received #{}", chunkNumber)
        log.info("-".repeat(80))
        log.info("  Timestamp:       {}", timestamp)
        log.info("  Stream:          {}", streamName)
        log.info("  Source:          {}/{}/{}", cloudNamespace, microserviceName, podName)
        log.info("  Size:            {} bytes ({} KB)", length, String.format("%.2f", length / 1024.0))
        log.info(
            "  Total Received:  {} bytes ({} MB in {} chunks)",
            totalBytesReceived.get(),
            String.format("%.2f", totalBytesReceived.get() / 1024.0 / 1024.0),
            chunkNumber
        )

        // Show data preview for small chunks or first bytes of large chunks
        if (length > 0) {
            val preview = getDataPreview(data, length)
            log.info("  Data Preview:")
            preview.split("\n").forEach { line ->
                log.info("    {}", line)
            }
        }

        log.info("=".repeat(80))
    }

    /**
     * Generate a preview of the data.
     * Shows hex dump and ASCII representation for the first bytes.
     */
    private fun getDataPreview(data: ByteArray, length: Int): String {
        val previewSize = minOf(length, 256) // Show first 256 bytes max
        val sb = StringBuilder()

        // Try to detect if data is text
        val isLikelyText = isLikelyText(data, previewSize)

        if (isLikelyText) {
            // Show as text
            val text = String(data, 0, previewSize, StandardCharsets.UTF_8)
            sb.append("[Text preview]\n")
            text.split("\n").take(10).forEach { line ->
                val truncated = if (line.length > 100) line.substring(0, 100) + "..." else line
                sb.append(truncated).append("\n")
            }
            if (length > previewSize) {
                sb.append("... (${length - previewSize} more bytes)")
            }
        } else {
            // Show as hex dump
            sb.append("[Hex dump - first $previewSize bytes]\n")
            for (i in 0 until previewSize step 16) {
                // Offset
                sb.append(String.format("%04X: ", i))

                // Hex values
                for (j in 0 until 16) {
                    if (i + j < previewSize) {
                        sb.append(String.format("%02X ", data[i + j]))
                    } else {
                        sb.append("   ")
                    }
                }

                sb.append(" | ")

                // ASCII representation
                for (j in 0 until 16) {
                    if (i + j < previewSize) {
                        val byte = data[i + j].toInt() and 0xFF
                        val char = if (byte in 32..126) byte.toChar() else '.'
                        sb.append(char)
                    }
                }

                sb.append("\n")
            }
            if (length > previewSize) {
                sb.append("... (${length - previewSize} more bytes)")
            }
        }

        return sb.toString()
    }

    /**
     * Heuristic to detect if data is likely text.
     */
    private fun isLikelyText(data: ByteArray, length: Int): Boolean {
        var printableCount = 0
        val sampleSize = minOf(length, 100)

        for (i in 0 until sampleSize) {
            val byte = data[i].toInt() and 0xFF
            // Count printable ASCII characters, newlines, tabs
            if (byte in 32..126 || byte == 9 || byte == 10 || byte == 13) {
                printableCount++
            }
        }

        // If more than 80% are printable, consider it text
        return printableCount.toDouble() / sampleSize > 0.8
    }

    /**
     * Get total statistics.
     */
    fun getStatistics(): String {
        return "Total: ${totalChunksReceived.get()} chunks, " +
                "${totalBytesReceived.get()} bytes " +
                "(${String.format("%.2f", totalBytesReceived.get() / 1024.0 / 1024.0)} MB)"
    }
}
