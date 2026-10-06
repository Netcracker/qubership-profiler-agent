package com.netcracker.profiler

import com.netcracker.profiler.agent.DumperPlugin_02
import com.netcracker.profiler.agent.LocalBuffer
import com.netcracker.profiler.agent.LocalState
import java.io.File
import java.util.concurrent.BlockingQueue
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import kotlin.concurrent.thread

/**
 * This is a no-op implementation for benchmark purposes.
 * The dumper will not write data to disk, and it would reset the buffers only.
 */
class NoopDumperPlugin : DumperPlugin_02 {
    override fun newDumper(
        dirtyBuffers: BlockingQueue<LocalBuffer>,
        emptyBuffers: BlockingQueue<LocalBuffer>,
        activeThreads: ConcurrentMap<Thread, LocalState>
    ) {
        thread {
            while (true) {
                val buffer = dirtyBuffers.take()
                buffer.reset()
                emptyBuffers.put(buffer)
            }
        }
    }

    override fun newDumper(
        dirtyBuffers: BlockingQueue<LocalBuffer>,
        emptyBuffers: BlockingQueue<LocalBuffer>,
        buffers: ArrayList<LocalBuffer>
    ) {
        newDumper(dirtyBuffers, emptyBuffers, ConcurrentHashMap())
    }

    override fun reconfigure() {
        TODO("Not yet implemented")
    }

    override fun getCurrentRoot(): File {
        TODO("Not yet implemented")
    }

    override fun getTags(): MutableList<String> {
        TODO("Not yet implemented")
    }

    override fun start(): Boolean {
        TODO("Not yet implemented")
    }

    override fun stop(force: Boolean): Boolean {
        TODO("Not yet implemented")
    }

    override fun isStarted(): Boolean {
        TODO("Not yet implemented")
    }

    override fun getNumberOfRestarts(): Int {
        TODO("Not yet implemented")
    }

    override fun getWrittenRecords(): Long {
        TODO("Not yet implemented")
    }

    override fun getWrittenBytes(): Long {
        TODO("Not yet implemented")
    }

    override fun getWriteTime(): Long {
        TODO("Not yet implemented")
    }

    override fun getDumperStartTime(): Long {
        TODO("Not yet implemented")
    }
}
