package org.qubership.profiler.agent

import io.mockk.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.RepeatedTest
import org.junit.jupiter.api.Timeout
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlin.random.Random

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class LocalBufferThreadSafetyTest {

    private lateinit var localBuffer: LocalBuffer
    private lateinit var mockLocalState: LocalState
    private val testThreads = 4
    private val operationsPerThread = 1000

    @BeforeEach
    fun setUp() {
        clearAllMocks()
        mockLocalState = mockk(relaxed = true)
        localBuffer = LocalBuffer()
        localBuffer.state = mockLocalState
        
        // Mock ProfilerData static methods
        mockkStatic(ProfilerData::class)
        every { ProfilerData.getEmptyBuffer(any()) } returns LocalBuffer()
        every { ProfilerData.addDirtyBuffer(any(), any()) } returns true
        
        // Mock Profiler static methods
        mockkStatic(Profiler::class)
        every { Profiler.exchangeBuffer(any()) } just Runs
        every { Profiler.exchangeBuffer(any(), any()) } just Runs
        
        // Mock TimerCache
        mockkStatic(TimerCache::class)
        every { TimerCache.now } returns System.currentTimeMillis()
        every { TimerCache.timer } returns System.currentTimeMillis().toInt()
        every { TimerCache.timerSHL32 } returns (System.currentTimeMillis() shl 32)
    }

    @Test
    @Timeout(10)
    fun `concurrent event calls should maintain count consistency`() {
        val executor = Executors.newFixedThreadPool(testThreads)
        val barrier = CyclicBarrier(testThreads)
        val exceptions = ConcurrentLinkedQueue<Exception>()
        
        val tasks = (1..testThreads).map { threadId ->
            executor.submit {
                try {
                    barrier.await()
                    repeat(operationsPerThread) { iteration ->
                        localBuffer.event("test-$threadId-$iteration", threadId)
                    }
                } catch (e: Exception) {
                    exceptions.add(e)
                }
            }
        }
        
        tasks.forEach { it.get() }
        executor.shutdown()
        
        assertTrue(exceptions.isEmpty(), "Exceptions occurred: ${exceptions.joinToString()}")
        assertTrue(localBuffer.count >= 0, "Count should be non-negative: ${localBuffer.count}")
        assertTrue(localBuffer.count <= LocalBuffer.SIZE, "Count should not exceed SIZE: ${localBuffer.count}")
    }

    @Test
    @Timeout(10)
    fun `concurrent initEnter calls should maintain count consistency`() {
        val executor = Executors.newFixedThreadPool(testThreads)
        val barrier = CyclicBarrier(testThreads)
        val exceptions = ConcurrentLinkedQueue<Exception>()
        
        val tasks = (1..testThreads).map { threadId ->
            executor.submit {
                try {
                    barrier.await()
                    repeat(operationsPerThread) { iteration ->
                        localBuffer.initEnter(threadId.toLong() or (iteration.toLong() shl 32))
                    }
                } catch (e: Exception) {
                    exceptions.add(e)
                }
            }
        }
        
        tasks.forEach { it.get() }
        executor.shutdown()
        
        assertTrue(exceptions.isEmpty(), "Exceptions occurred: ${exceptions.joinToString()}")
        assertTrue(localBuffer.count >= 0, "Count should be non-negative: ${localBuffer.count}")
        assertTrue(localBuffer.count <= LocalBuffer.SIZE, "Count should not exceed SIZE: ${localBuffer.count}")
    }

    @Test
    @Timeout(10)
    fun `mixed concurrent operations should maintain count consistency`() {
        val executor = Executors.newFixedThreadPool(testThreads)
        val barrier = CyclicBarrier(testThreads)
        val exceptions = ConcurrentLinkedQueue<Exception>()
        
        val tasks = (1..testThreads).map { threadId ->
            executor.submit {
                try {
                    barrier.await()
                    repeat(operationsPerThread / 2) { iteration ->
                        if (Random.nextBoolean()) {
                            localBuffer.event("test-$threadId-$iteration", threadId)
                        } else {
                            localBuffer.initEnter(threadId.toLong() or (iteration.toLong() shl 32))
                        }
                    }
                } catch (e: Exception) {
                    exceptions.add(e)
                }
            }
        }
        
        tasks.forEach { it.get() }
        executor.shutdown()
        
        assertTrue(exceptions.isEmpty(), "Exceptions occurred: ${exceptions.joinToString()}")
        assertTrue(localBuffer.count >= 0, "Count should be non-negative: ${localBuffer.count}")
        assertTrue(localBuffer.count <= LocalBuffer.SIZE, "Count should not exceed SIZE: ${localBuffer.count}")
    }

    @Test
    @Timeout(10) 
    fun `concurrent writes with reader should maintain data consistency`() {
        val executor = Executors.newFixedThreadPool(testThreads + 1)
        val barrier = CyclicBarrier(testThreads + 1)
        val exceptions = ConcurrentLinkedQueue<Exception>()
        val readValues = ConcurrentLinkedQueue<Pair<Int, Int>>()
        
        // Reader thread
        val readerTask = executor.submit {
            try {
                barrier.await()
                repeat(operationsPerThread) {
                    val count = localBuffer.count
                    val first = localBuffer.first
                    readValues.add(Pair(count, first))
                    Thread.sleep(1) // Small delay to increase chance of race conditions
                }
            } catch (e: Exception) {
                exceptions.add(e)
            }
        }
        
        // Writer threads
        val writerTasks = (1..testThreads).map { threadId ->
            executor.submit {
                try {
                    barrier.await()
                    repeat(operationsPerThread / testThreads) { iteration ->
                        localBuffer.event("test-$threadId-$iteration", threadId)
                    }
                } catch (e: Exception) {
                    exceptions.add(e)
                }
            }
        }
        
        (writerTasks + readerTask).forEach { it.get() }
        executor.shutdown()
        
        assertTrue(exceptions.isEmpty(), "Exceptions occurred: ${exceptions.joinToString()}")
        
        // Verify all read values are consistent
        readValues.forEach { (count, first) ->
            assertTrue(count >= 0, "Count should be non-negative: $count")
            assertTrue(first >= 0, "First should be non-negative: $first")
            assertTrue(count <= LocalBuffer.SIZE, "Count should not exceed SIZE: $count")
            assertTrue(first <= count, "First should not exceed count: first=$first, count=$count")
        }
    }

    @RepeatedTest(10)
    @Timeout(5)
    fun `buffer exchange should be thread safe`() {
        val exchangeCount = AtomicInteger(0)
        val executor = Executors.newFixedThreadPool(testThreads)
        val barrier = CyclicBarrier(testThreads)
        
        // Mock exchange to count invocations
        every { Profiler.exchangeBuffer(any()) } answers {
            exchangeCount.incrementAndGet()
            val buffer = firstArg<LocalBuffer>()
            buffer.count = 0 // Reset count as real exchange would
        }
        
        val tasks = (1..testThreads).map { threadId ->
            executor.submit {
                barrier.await()
                repeat(LocalBuffer.SIZE + 10) { iteration ->
                    localBuffer.event("test-$threadId-$iteration", threadId)
                }
            }
        }
        
        tasks.forEach { it.get() }
        executor.shutdown()
        
        // Should have triggered at least one exchange
        assertTrue(exchangeCount.get() > 0, "Buffer exchange should have been triggered")
    }

    @Test
    @Timeout(10)
    fun `reset should be thread safe with concurrent access`() {
        val executor = Executors.newFixedThreadPool(testThreads + 1)
        val barrier = CyclicBarrier(testThreads + 1)
        val exceptions = ConcurrentLinkedQueue<Exception>()
        
        // Fill buffer first
        repeat(100) { localBuffer.event("initial-$it", it) }
        
        // Reset thread
        val resetTask = executor.submit {
            try {
                barrier.await()
                repeat(10) {
                    localBuffer.reset()
                    Thread.sleep(10)
                }
            } catch (e: Exception) {
                exceptions.add(e)
            }
        }
        
        // Reader threads
        val readerTasks = (1..testThreads).map { threadId ->
            executor.submit {
                try {
                    barrier.await()
                    repeat(100) {
                        val count = localBuffer.count
                        val first = localBuffer.first
                        // Verify consistency
                        assertTrue(count >= 0, "Count should be non-negative: $count")
                        assertTrue(first >= 0, "First should be non-negative: $first")
                        assertTrue(first <= count, "First should not exceed count: first=$first, count=$count")
                    }
                } catch (e: Exception) {
                    exceptions.add(e)
                }
            }
        }
        
        (readerTasks + resetTask).forEach { it.get() }
        executor.shutdown()
        
        assertTrue(exceptions.isEmpty(), "Exceptions occurred: ${exceptions.joinToString()}")
    }

    @Test
    @Timeout(10)
    fun `memory visibility of count updates should be consistent`() {
        val executor = Executors.newFixedThreadPool(2)
        val countValues = ConcurrentLinkedQueue<Int>()
        val barrier = CyclicBarrier(2)
        
        val writerTask = executor.submit {
            barrier.await()
            repeat(1000) { iteration ->
                localBuffer.event("test-$iteration", iteration)
                Thread.yield() // Encourage context switch
            }
        }
        
        val readerTask = executor.submit {
            barrier.await()
            repeat(1000) {
                countValues.add(localBuffer.count)
                Thread.yield() // Encourage context switch
            }
        }
        
        listOf(writerTask, readerTask).forEach { it.get() }
        executor.shutdown()
        
        // Verify count values are monotonically increasing (within buffer size)
        val sortedCounts = countValues.toList().windowed(2)
        var violations = 0
        sortedCounts.forEach { (prev, curr) ->
            if (curr < prev && curr != 0) { // curr can be 0 if buffer was exchanged
                violations++
            }
        }
        
        // Allow some violations due to buffer exchange, but not too many
        assertTrue(violations < countValues.size * 0.1, 
            "Too many count ordering violations: $violations out of ${countValues.size}")
    }
}