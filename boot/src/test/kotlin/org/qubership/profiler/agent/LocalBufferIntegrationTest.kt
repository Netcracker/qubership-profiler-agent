package org.qubership.profiler.agent

import io.mockk.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Timeout
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.random.Random

/**
 * Integration tests for LocalBuffer that simulate real-world usage patterns
 * with proper ProfilerData and Profiler interactions.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class LocalBufferIntegrationTest {

    private lateinit var localBuffer: LocalBuffer
    private lateinit var mockLocalState: LocalState
    private val exchangeCount = AtomicInteger(0)
    private val dirtyBuffers = LinkedBlockingQueue<LocalBuffer>()
    private val emptyBuffers = LinkedBlockingQueue<LocalBuffer>()

    @BeforeEach
    fun setUp() {
        clearAllMocks()
        setupMocksAndQueues()
    }

    private fun setupMocksAndQueues() {
        // Reset counters
        exchangeCount.set(0)
        dirtyBuffers.clear()
        emptyBuffers.clear()

        // Fill empty buffers queue
        repeat(10) { emptyBuffers.offer(LocalBuffer()) }

        // Setup LocalState
        mockLocalState = mockk(relaxed = true)
        every { mockLocalState.isSystem } returns false
        
        localBuffer = LocalBuffer()
        localBuffer.state = mockLocalState

        // Mock ProfilerData
        mockkStatic(ProfilerData::class)
        every { ProfilerData.dumperDead } returns false
        every { ProfilerData.getEmptyBuffer(any()) } answers {
            emptyBuffers.poll() ?: LocalBuffer()
        }
        every { ProfilerData.addDirtyBuffer(any(), any()) } answers {
            val buffer = firstArg<LocalBuffer>()
            dirtyBuffers.offer(buffer)
            true
        }
        every { ProfilerData.addEmptyBuffer(any()) } answers {
            val buffer = firstArg<LocalBuffer>()
            emptyBuffers.offer(buffer)
        }

        // Mock Profiler.exchangeBuffer
        mockkStatic(Profiler::class)
        every { Profiler.exchangeBuffer(any()) } answers {
            exchangeCount.incrementAndGet()
            val buffer = firstArg<LocalBuffer>()
            
            // Simulate real exchange behavior
            val newBuffer = ProfilerData.getEmptyBuffer(buffer.state)
            newBuffer.init(buffer)
            
            if (!ProfilerData.addDirtyBuffer(buffer, true)) {
                buffer.reset()
                newBuffer.reset()
                ProfilerData.addEmptyBuffer(newBuffer)
                return@answers
            }
            
            buffer.state.buffer = newBuffer
        }
        
        every { Profiler.exchangeBuffer(any(), any()) } answers {
            exchangeCount.incrementAndGet()
            val buffer = firstArg<LocalBuffer>()
            val methodAndTime = secondArg<Long>()
            
            // Simulate real exchange behavior with timed entry
            val newBuffer = ProfilerData.getEmptyBuffer(buffer.state)
            newBuffer.init(buffer)
            
            if (!ProfilerData.addDirtyBuffer(buffer, true)) {
                buffer.reset()
                newBuffer.reset()
                ProfilerData.addEmptyBuffer(newBuffer)
                return@answers
            }
            
            buffer.state.buffer = newBuffer
            newBuffer.initTimedEnter(methodAndTime)
        }

        // Mock TimerCache
        mockkStatic(TimerCache::class)
        every { TimerCache.now } returns System.currentTimeMillis()
        every { TimerCache.timer } returns System.currentTimeMillis().toInt()
        every { TimerCache.timerSHL32 } returns (System.currentTimeMillis() shl 32)
    }

    @Test
    @Timeout(30)
    fun `high frequency multi-threaded producer consumer simulation`() {
        val producerThreads = 8
        val consumerThreads = 2
        val operationsPerProducer = 2000
        val executor = Executors.newFixedThreadPool(producerThreads + consumerThreads)
        val barrier = CyclicBarrier(producerThreads + consumerThreads)
        val exceptions = ConcurrentLinkedQueue<Exception>()
        val consumedBuffers = AtomicInteger(0)
        val producedEvents = AtomicInteger(0)
        val testRunning = AtomicBoolean(true)

        // Producer threads - simulate worker threads
        val producerTasks = (1..producerThreads).map { threadId ->
            executor.submit {
                try {
                    barrier.await()
                    val threadBuffer = LocalBuffer()
                    threadBuffer.state = mockLocalState
                    
                    repeat(operationsPerProducer) { iteration ->
                        when (Random.nextInt(3)) {
                            0 -> threadBuffer.event("event-$threadId-$iteration", threadId)
                            1 -> threadBuffer.initEnter(threadId.toLong() or (iteration.toLong() shl 32))
                            2 -> threadBuffer.initTimedExit(System.currentTimeMillis() shl 32)
                        }
                        producedEvents.incrementAndGet()
                        
                        // Occasionally sleep to simulate real work
                        if (iteration % 100 == 0) {
                            Thread.sleep(1)
                        }
                    }
                } catch (e: Exception) {
                    exceptions.add(e)
                }
            }
        }

        // Consumer threads - simulate dumper threads
        val consumerTasks = (1..consumerThreads).map { threadId ->
            executor.submit {
                try {
                    barrier.await()
                    
                    while (testRunning.get() || !dirtyBuffers.isEmpty()) {
                        val buffer = dirtyBuffers.poll(100, TimeUnit.MILLISECONDS)
                        if (buffer != null) {
                            consumedBuffers.incrementAndGet()
                            
                            // Simulate reading buffer data
                            val count = buffer.count
                            val first = buffer.first
                            
                            // Verify buffer consistency
                            assertTrue(count >= 0, "Count should be non-negative: $count")
                            assertTrue(first >= 0, "First should be non-negative: $first")
                            assertTrue(first <= count, "First should not exceed count: first=$first, count=$count")
                            
                            // Simulate processing time
                            Thread.sleep(Random.nextLong(1, 5))
                            
                            // Return buffer to empty queue
                            buffer.reset()
                            emptyBuffers.offer(buffer)
                        }
                    }
                } catch (e: Exception) {
                    exceptions.add(e)
                }
            }
        }

        // Wait for producers to complete
        producerTasks.forEach { it.get() }
        
        // Allow consumers to drain the queue
        Thread.sleep(1000)
        testRunning.set(false)
        
        // Wait for consumers to complete
        consumerTasks.forEach { it.get() }
        executor.shutdown()

        // Verify no exceptions occurred
        assertTrue(exceptions.isEmpty(), "Exceptions occurred: ${exceptions.joinToString()}")
        
        // Verify some work was done
        assertTrue(producedEvents.get() > 0, "Should have produced events")
        assertTrue(consumedBuffers.get() > 0, "Should have consumed buffers")
        
        println("Produced ${producedEvents.get()} events, consumed ${consumedBuffers.get()} buffers, ${exchangeCount.get()} exchanges")
    }

    @Test
    @Timeout(15)
    fun `buffer exchange triggers correctly at capacity`() {
        val executor = Executors.newFixedThreadPool(4)
        val barrier = CyclicBarrier(4)
        val exceptions = ConcurrentLinkedQueue<Exception>()
        
        val tasks = (1..4).map { threadId ->
            executor.submit {
                try {
                    barrier.await()
                    val threadBuffer = LocalBuffer()
                    threadBuffer.state = mockLocalState
                    
                    // Fill buffer beyond capacity to trigger exchange
                    repeat(LocalBuffer.SIZE + 50) { iteration ->
                        threadBuffer.event("test-$threadId-$iteration", threadId)
                    }
                } catch (e: Exception) {
                    exceptions.add(e)
                }
            }
        }
        
        tasks.forEach { it.get() }
        executor.shutdown()
        
        assertTrue(exceptions.isEmpty(), "Exceptions occurred: ${exceptions.joinToString()}")
        assertTrue(exchangeCount.get() > 0, "Buffer exchange should have been triggered")
        assertTrue(dirtyBuffers.isNotEmpty(), "Should have dirty buffers")
    }

    @Test
    @Timeout(10)
    fun `buffer reset during concurrent access maintains consistency`() {
        val executor = Executors.newFixedThreadPool(6)
        val barrier = CyclicBarrier(6)
        val exceptions = ConcurrentLinkedQueue<Exception>()
        val inconsistentReads = AtomicInteger(0)
        
        // Pre-populate buffer
        repeat(100) { localBuffer.event("initial-$it", it) }
        
        // Reset thread
        val resetTask = executor.submit {
            try {
                barrier.await()
                repeat(20) {
                    localBuffer.reset()
                    Thread.sleep(5)
                }
            } catch (e: Exception) {
                exceptions.add(e)
            }
        }
        
        // Writer threads
        val writerTasks = (1..2).map { threadId ->
            executor.submit {
                try {
                    barrier.await()
                    repeat(200) { iteration ->
                        localBuffer.event("writer-$threadId-$iteration", threadId)
                        Thread.yield()
                    }
                } catch (e: Exception) {
                    exceptions.add(e)
                }
            }
        }
        
        // Reader threads
        val readerTasks = (1..3).map { threadId ->
            executor.submit {
                try {
                    barrier.await()
                    repeat(300) {
                        val count = localBuffer.count
                        val first = localBuffer.first
                        
                        // Check for inconsistency
                        if (first > count || count < 0 || first < 0) {
                            inconsistentReads.incrementAndGet()
                        }
                        
                        Thread.yield()
                    }
                } catch (e: Exception) {
                    exceptions.add(e)
                }
            }
        }
        
        (listOf(resetTask) + writerTasks + readerTasks).forEach { it.get() }
        executor.shutdown()
        
        assertTrue(exceptions.isEmpty(), "Exceptions occurred: ${exceptions.joinToString()}")
        assertEquals(0, inconsistentReads.get(), "Should not have inconsistent reads")
    }

    @Test
    @Timeout(10)
    fun `dumper dead scenario handles gracefully`() {
        // Simulate dumper death
        every { ProfilerData.dumperDead } returns true
        
        val executor = Executors.newFixedThreadPool(4)
        val barrier = CyclicBarrier(4)
        val exceptions = ConcurrentLinkedQueue<Exception>()
        
        val tasks = (1..4).map { threadId ->
            executor.submit {
                try {
                    barrier.await()
                    val threadBuffer = LocalBuffer()
                    threadBuffer.state = mockLocalState
                    
                    // Try to trigger exchange - should reset instead
                    repeat(LocalBuffer.SIZE + 10) { iteration ->
                        threadBuffer.event("test-$threadId-$iteration", threadId)
                    }
                } catch (e: Exception) {
                    exceptions.add(e)
                }
            }
        }
        
        tasks.forEach { it.get() }
        executor.shutdown()
        
        assertTrue(exceptions.isEmpty(), "Exceptions occurred: ${exceptions.joinToString()}")
        
        // Should not have added any dirty buffers when dumper is dead
        assertTrue(dirtyBuffers.isEmpty(), "Should not have dirty buffers when dumper is dead")
    }

    @Test
    @Timeout(10)
    fun `system thread does not participate in buffer exchange`() {
        // Mark as system thread
        every { mockLocalState.isSystem } returns true
        
        val executor = Executors.newFixedThreadPool(2)
        val barrier = CyclicBarrier(2)
        val exceptions = ConcurrentLinkedQueue<Exception>()
        
        val tasks = (1..2).map { threadId ->
            executor.submit {
                try {
                    barrier.await()
                    val threadBuffer = LocalBuffer()
                    threadBuffer.state = mockLocalState
                    
                    // Try to trigger exchange - should reset instead
                    repeat(LocalBuffer.SIZE + 10) { iteration ->
                        threadBuffer.event("test-$threadId-$iteration", threadId)
                    }
                } catch (e: Exception) {
                    exceptions.add(e)
                }
            }
        }
        
        tasks.forEach { it.get() }
        executor.shutdown()
        
        assertTrue(exceptions.isEmpty(), "Exceptions occurred: ${exceptions.joinToString()}")
        
        // System threads should not trigger exchange
        assertEquals(0, exchangeCount.get(), "System threads should not trigger exchange")
        assertTrue(dirtyBuffers.isEmpty(), "Should not have dirty buffers from system threads")
    }
}