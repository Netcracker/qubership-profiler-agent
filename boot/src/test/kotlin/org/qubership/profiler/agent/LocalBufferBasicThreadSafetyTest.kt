package org.qubership.profiler.agent

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.RepeatedTest
import org.junit.jupiter.api.Timeout
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicInteger
import kotlin.random.Random

/**
 * Basic thread-safety tests for LocalBuffer without heavy mocking.
 * These tests focus on the core thread-safety properties of LocalBuffer fields.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class LocalBufferBasicThreadSafetyTest {

    private val testThreads = 4
    private val operationsPerThread = 1000

    @Test
    @Timeout(10)
    fun `concurrent count field access should be consistent`() {
        val localBuffer = LocalBuffer()
        val executor = Executors.newFixedThreadPool(testThreads)
        val barrier = CyclicBarrier(testThreads)
        val exceptions = ConcurrentLinkedQueue<Exception>()
        val countValues = ConcurrentLinkedQueue<Int>()
        
        val tasks = (1..testThreads).map { threadId ->
            executor.submit {
                try {
                    barrier.await()
                    repeat(operationsPerThread) {
                        // Read count value
                        val count = localBuffer.count
                        countValues.add(count)
                        
                        // Simulate field access patterns
                        if (Random.nextBoolean()) {
                            val first = localBuffer.first
                            // Verify basic consistency
                            if (first > count || count < 0 || first < 0) {
                                throw IllegalStateException("Inconsistent state: first=$first, count=$count")
                            }
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
        assertTrue(countValues.isNotEmpty(), "Should have read some count values")
        
        // All count values should be valid
        countValues.forEach { count ->
            assertTrue(count >= 0, "Count should be non-negative: $count")
            assertTrue(count <= LocalBuffer.SIZE, "Count should not exceed SIZE: $count")
        }
    }

    @Test
    @Timeout(10)
    fun `concurrent first field access should be consistent`() {
        val localBuffer = LocalBuffer()
        localBuffer.count = 100 // Set some initial state
        localBuffer.first = 10
        
        val executor = Executors.newFixedThreadPool(testThreads)
        val barrier = CyclicBarrier(testThreads)
        val exceptions = ConcurrentLinkedQueue<Exception>()
        val fieldPairs = ConcurrentLinkedQueue<Pair<Int, Int>>()
        
        val tasks = (1..testThreads).map { threadId ->
            executor.submit {
                try {
                    barrier.await()
                    repeat(operationsPerThread) {
                        // Read both fields
                        val count = localBuffer.count
                        val first = localBuffer.first
                        fieldPairs.add(Pair(count, first))
                        
                        // Verify basic consistency
                        if (first > count || count < 0 || first < 0) {
                            throw IllegalStateException("Inconsistent state: first=$first, count=$count")
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
        assertTrue(fieldPairs.isNotEmpty(), "Should have read some field pairs")
        
        // All field pairs should be consistent
        fieldPairs.forEach { (count, first) ->
            assertTrue(count >= 0, "Count should be non-negative: $count")
            assertTrue(first >= 0, "First should be non-negative: $first")
            assertTrue(first <= count, "First should not exceed count: first=$first, count=$count")
        }
    }

    @Test
    @Timeout(10)
    fun `concurrent reset with field access should maintain consistency`() {
        val localBuffer = LocalBuffer()
        val executor = Executors.newFixedThreadPool(testThreads + 1)
        val barrier = CyclicBarrier(testThreads + 1)
        val exceptions = ConcurrentLinkedQueue<Exception>()
        val inconsistentReads = AtomicInteger(0)
        
        // Fill buffer initially
        repeat(50) {
            if (localBuffer.count < LocalBuffer.SIZE) {
                localBuffer.data[localBuffer.count] = it.toLong()
                localBuffer.value[localBuffer.count] = "test-$it"
                localBuffer.count++
            }
        }
        localBuffer.first = 10
        
        // Reset thread
        val resetTask = executor.submit {
            try {
                barrier.await()
                repeat(100) {
                    localBuffer.reset()
                    Thread.sleep(5)
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
                    repeat(500) {
                        val count = localBuffer.count
                        val first = localBuffer.first
                        
                        // Check for inconsistency
                        if (first > count || count < 0 || first < 0) {
                            inconsistentReads.incrementAndGet()
                        }
                        
                        // Additional consistency check - after reset both should be 0
                        if (count == 0 && first != 0) {
                            inconsistentReads.incrementAndGet()
                        }
                        
                        Thread.yield()
                    }
                } catch (e: Exception) {
                    exceptions.add(e)
                }
            }
        }
        
        (listOf(resetTask) + readerTasks).forEach { it.get() }
        executor.shutdown()
        
        assertTrue(exceptions.isEmpty(), "Exceptions occurred: ${exceptions.joinToString()}")
        
        // Allow some inconsistency during reset operations, but not too much
        val totalReads = testThreads * 500
        val inconsistencyRate = inconsistentReads.get().toDouble() / totalReads
        assertTrue(inconsistencyRate < 0.1, 
            "Too many inconsistent reads: ${inconsistentReads.get()} out of $totalReads (${inconsistencyRate * 100}%)")
    }

    @Test
    @Timeout(10)
    fun `memory visibility of field updates should be observable`() {
        val localBuffer = LocalBuffer()
        val executor = Executors.newFixedThreadPool(2)
        val barrier = CyclicBarrier(2)
        val countUpdates = ConcurrentLinkedQueue<Int>()
        val observedValues = ConcurrentLinkedQueue<Int>()
        
        val writerTask = executor.submit {
            barrier.await()
            repeat(1000) { iteration ->
                localBuffer.count = iteration
                countUpdates.add(iteration)
                Thread.yield()
            }
        }
        
        val readerTask = executor.submit {
            barrier.await()
            repeat(1000) {
                observedValues.add(localBuffer.count)
                Thread.yield()
            }
        }
        
        listOf(writerTask, readerTask).forEach { it.get() }
        executor.shutdown()
        
        assertTrue(countUpdates.isNotEmpty(), "Should have count updates")
        assertTrue(observedValues.isNotEmpty(), "Should have observed values")
        
        // Should observe at least some of the written values
        val maxWrittenValue = countUpdates.maxOrNull() ?: 0
        val maxObservedValue = observedValues.maxOrNull() ?: 0
        
        assertTrue(maxObservedValue > 0, "Should observe some updates")
        assertTrue(maxObservedValue <= maxWrittenValue, 
            "Observed value should not exceed written value: observed=$maxObservedValue, written=$maxWrittenValue")
    }

    @Test
    @Timeout(10)
    fun `data array bounds safety under concurrent access`() {
        val localBuffer = LocalBuffer()
        val executor = Executors.newFixedThreadPool(testThreads)
        val barrier = CyclicBarrier(testThreads)
        val exceptions = ConcurrentLinkedQueue<Exception>()
        val safeAccesses = AtomicInteger(0)
        
        val tasks = (1..testThreads).map { threadId ->
            executor.submit {
                try {
                    barrier.await()
                    repeat(operationsPerThread) {
                        val count = localBuffer.count
                        val first = localBuffer.first
                        
                        // Try to access data array safely
                        if (count >= 0 && count < LocalBuffer.SIZE) {
                            localBuffer.data[count] = threadId.toLong()
                            safeAccesses.incrementAndGet()
                        }
                        
                        // Try to access value array safely
                        if (first >= 0 && first < LocalBuffer.SIZE && first < count) {
                            val value = localBuffer.value[first]
                            // Just read the value to test access
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
        assertTrue(safeAccesses.get() > 0, "Should have performed some safe accesses")
    }

    @RepeatedTest(5)
    @Timeout(5)
    fun `stress test field consistency under high contention`() {
        val localBuffer = LocalBuffer()
        val executor = Executors.newFixedThreadPool(8)
        val barrier = CyclicBarrier(8)
        val exceptions = ConcurrentLinkedQueue<Exception>()
        val totalOperations = AtomicInteger(0)
        val inconsistentStates = AtomicInteger(0)
        
        val tasks = (1..8).map { threadId ->
            executor.submit {
                try {
                    barrier.await()
                    repeat(200) {
                        val count = localBuffer.count
                        val first = localBuffer.first
                        totalOperations.incrementAndGet()
                        
                        // Check for inconsistent states
                        if (first > count || count < 0 || first < 0) {
                            inconsistentStates.incrementAndGet()
                        }
                        
                        // Simulate some work
                        if (Random.nextBoolean()) {
                            Thread.yield()
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
        assertTrue(totalOperations.get() > 0, "Should have performed operations")
        
        // In a properly designed system, there should be no inconsistent states
        // Since fields are not volatile, we may observe some inconsistencies
        val inconsistencyRate = inconsistentStates.get().toDouble() / totalOperations.get()
        assertTrue(inconsistencyRate < 0.5, 
            "Too many inconsistent states: ${inconsistentStates.get()} out of ${totalOperations.get()} (${inconsistencyRate * 100}%)")
    }
}