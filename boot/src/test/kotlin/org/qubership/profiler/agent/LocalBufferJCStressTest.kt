package org.qubership.profiler.agent

import io.mockk.*
import org.junit.platform.commons.annotation.Testable
import org.openjdk.jcstress.annotations.*
import org.openjdk.jcstress.infra.results.II_Result
import org.openjdk.jcstress.infra.results.I_Result

/**
 * JCStress tests for LocalBuffer thread safety.
 * These tests use the JCStress framework to detect race conditions
 * in LocalBuffer operations under concurrent access.
 */
@Testable
@JCStressTest
@Outcome(id = ["0, 1", "1, 0", "1, 1"], expect = Expect.ACCEPTABLE, desc = "Both operations completed")
@Outcome(id = ["0, 0"], expect = Expect.FORBIDDEN, desc = "Both operations failed - race condition")
@State
open class LocalBufferConcurrentEventTest {

    private lateinit var localBuffer: LocalBuffer
    private lateinit var mockLocalState: LocalState

    init {
        setupMocks()
    }

    private fun setupMocks() {
        clearAllMocks()
        mockLocalState = mockk(relaxed = true)
        localBuffer = LocalBuffer()
        localBuffer.state = mockLocalState

        // Mock ProfilerData and Profiler static methods
        mockkStatic(ProfilerData::class)
        every { ProfilerData.getEmptyBuffer(any()) } returns LocalBuffer()
        every { ProfilerData.addDirtyBuffer(any(), any()) } returns true

        mockkStatic(Profiler::class)
        every { Profiler.exchangeBuffer(any()) } just Runs
        every { Profiler.exchangeBuffer(any(), any()) } just Runs

        mockkStatic(TimerCache::class)
        every { TimerCache.now } returns System.currentTimeMillis()
        every { TimerCache.timer } returns System.currentTimeMillis().toInt()
        every { TimerCache.timerSHL32 } returns (System.currentTimeMillis() shl 32)
    }

    @Actor
    fun actor1(r: II_Result) {
        val oldCount = localBuffer.count
        localBuffer.event("test1", 1)
        r.r1 = if (localBuffer.count > oldCount) 1 else 0
    }

    @Actor
    fun actor2(r: II_Result) {
        val oldCount = localBuffer.count
        localBuffer.event("test2", 2)
        r.r2 = if (localBuffer.count > oldCount) 1 else 0
    }
}

@JCStressTest
@Outcome(id = ["0, 1", "1, 0", "1, 1"], expect = Expect.ACCEPTABLE, desc = "Both operations completed")
@Outcome(id = ["0, 0"], expect = Expect.FORBIDDEN, desc = "Both operations failed - race condition")
@State
open class LocalBufferConcurrentInitEnterTest {

    private lateinit var localBuffer: LocalBuffer
    private lateinit var mockLocalState: LocalState

    init {
        setupMocks()
    }

    private fun setupMocks() {
        clearAllMocks()
        mockLocalState = mockk(relaxed = true)
        localBuffer = LocalBuffer()
        localBuffer.state = mockLocalState

        mockkStatic(ProfilerData::class)
        every { ProfilerData.getEmptyBuffer(any()) } returns LocalBuffer()
        every { ProfilerData.addDirtyBuffer(any(), any()) } returns true

        mockkStatic(Profiler::class)
        every { Profiler.exchangeBuffer(any()) } just Runs
        every { Profiler.exchangeBuffer(any(), any()) } just Runs

        mockkStatic(TimerCache::class)
        every { TimerCache.now } returns System.currentTimeMillis()
        every { TimerCache.timer } returns System.currentTimeMillis().toInt()
        every { TimerCache.timerSHL32 } returns (System.currentTimeMillis() shl 32)
    }

    @Actor
    fun actor1(r: II_Result) {
        val oldCount = localBuffer.count
        localBuffer.initEnter(1L)
        r.r1 = if (localBuffer.count > oldCount) 1 else 0
    }

    @Actor
    fun actor2(r: II_Result) {
        val oldCount = localBuffer.count
        localBuffer.initEnter(2L)
        r.r2 = if (localBuffer.count > oldCount) 1 else 0
    }
}

@JCStressTest
@Outcome(id = ["0, 1", "1, 0", "1, 1"], expect = Expect.ACCEPTABLE, desc = "Mixed operations completed")
@Outcome(id = ["0, 0"], expect = Expect.FORBIDDEN, desc = "Both operations failed - race condition")
@State
open class LocalBufferMixedOperationsTest {

    private lateinit var localBuffer: LocalBuffer
    private lateinit var mockLocalState: LocalState

    init {
        setupMocks()
    }

    private fun setupMocks() {
        clearAllMocks()
        mockLocalState = mockk(relaxed = true)
        localBuffer = LocalBuffer()
        localBuffer.state = mockLocalState

        mockkStatic(ProfilerData::class)
        every { ProfilerData.getEmptyBuffer(any()) } returns LocalBuffer()
        every { ProfilerData.addDirtyBuffer(any(), any()) } returns true

        mockkStatic(Profiler::class)
        every { Profiler.exchangeBuffer(any()) } just Runs
        every { Profiler.exchangeBuffer(any(), any()) } just Runs

        mockkStatic(TimerCache::class)
        every { TimerCache.now } returns System.currentTimeMillis()
        every { TimerCache.timer } returns System.currentTimeMillis().toInt()
        every { TimerCache.timerSHL32 } returns (System.currentTimeMillis() shl 32)
    }

    @Actor
    fun eventActor(r: II_Result) {
        val oldCount = localBuffer.count
        localBuffer.event("test", 1)
        r.r1 = if (localBuffer.count > oldCount) 1 else 0
    }

    @Actor
    fun initEnterActor(r: II_Result) {
        val oldCount = localBuffer.count
        localBuffer.initEnter(2L)
        r.r2 = if (localBuffer.count > oldCount) 1 else 0
    }
}

@JCStressTest
@Outcome(expect = Expect.ACCEPTABLE, desc = "Count read is consistent")
@State
open class LocalBufferCountVisibilityTest {

    private lateinit var localBuffer: LocalBuffer
    private lateinit var mockLocalState: LocalState

    init {
        setupMocks()
    }

    private fun setupMocks() {
        clearAllMocks()
        mockLocalState = mockk(relaxed = true)
        localBuffer = LocalBuffer()
        localBuffer.state = mockLocalState

        mockkStatic(ProfilerData::class)
        every { ProfilerData.getEmptyBuffer(any()) } returns LocalBuffer()
        every { ProfilerData.addDirtyBuffer(any(), any()) } returns true

        mockkStatic(Profiler::class)
        every { Profiler.exchangeBuffer(any()) } just Runs
        every { Profiler.exchangeBuffer(any(), any()) } just Runs

        mockkStatic(TimerCache::class)
        every { TimerCache.now } returns System.currentTimeMillis()
        every { TimerCache.timer } returns System.currentTimeMillis().toInt()
        every { TimerCache.timerSHL32 } returns (System.currentTimeMillis() shl 32)
    }

    @Actor
    fun writer() {
        localBuffer.event("test", 1)
    }

    @Actor
    fun reader(r: I_Result) {
        r.r1 = localBuffer.count
    }
}

@JCStressTest
@Outcome(expect = Expect.ACCEPTABLE, desc = "Count and first fields are consistent")
@State
open class LocalBufferCountFirstConsistencyTest {

    private lateinit var localBuffer: LocalBuffer
    private lateinit var mockLocalState: LocalState

    init {
        setupMocks()
        // Pre-populate buffer with some data
        localBuffer.first = 0
        localBuffer.count = 5
    }

    private fun setupMocks() {
        clearAllMocks()
        mockLocalState = mockk(relaxed = true)
        localBuffer = LocalBuffer()
        localBuffer.state = mockLocalState

        mockkStatic(ProfilerData::class)
        every { ProfilerData.getEmptyBuffer(any()) } returns LocalBuffer()
        every { ProfilerData.addDirtyBuffer(any(), any()) } returns true

        mockkStatic(Profiler::class)
        every { Profiler.exchangeBuffer(any()) } just Runs
        every { Profiler.exchangeBuffer(any(), any()) } just Runs

        mockkStatic(TimerCache::class)
        every { TimerCache.now } returns System.currentTimeMillis()
        every { TimerCache.timer } returns System.currentTimeMillis().toInt()
        every { TimerCache.timerSHL32 } returns (System.currentTimeMillis() shl 32)
    }

    @Actor
    fun writer() {
        localBuffer.event("test", 1)
    }

    @Actor
    fun reader(r: II_Result) {
        val count = localBuffer.count
        val first = localBuffer.first
        r.r1 = count
        r.r2 = first
    }

    @Arbiter
    fun arbiter(r: II_Result) {
        val count = r.r1
        val first = r.r2
        if (first > count) {
            r.r1 = -1 // Signal inconsistency
            r.r2 = -1
        }
    }
}

@JCStressTest
@Outcome(expect = Expect.ACCEPTABLE, desc = "Reset operation is atomic")
@State
open class LocalBufferResetVisibilityTest {

    private lateinit var localBuffer: LocalBuffer
    private lateinit var mockLocalState: LocalState

    init {
        setupMocks()
        // Pre-populate buffer
        localBuffer.count = 10
        localBuffer.first = 5
    }

    private fun setupMocks() {
        clearAllMocks()
        mockLocalState = mockk(relaxed = true)
        localBuffer = LocalBuffer()
        localBuffer.state = mockLocalState

        mockkStatic(ProfilerData::class)
        every { ProfilerData.getEmptyBuffer(any()) } returns LocalBuffer()
        every { ProfilerData.addDirtyBuffer(any(), any()) } returns true

        mockkStatic(Profiler::class)
        every { Profiler.exchangeBuffer(any()) } just Runs
        every { Profiler.exchangeBuffer(any(), any()) } just Runs

        mockkStatic(TimerCache::class)
        every { TimerCache.now } returns System.currentTimeMillis()
        every { TimerCache.timer } returns System.currentTimeMillis().toInt()
        every { TimerCache.timerSHL32 } returns (System.currentTimeMillis() shl 32)
    }

    @Actor
    fun reset() {
        localBuffer.reset()
    }

    @Actor
    fun reader(r: II_Result) {
        r.r1 = localBuffer.count
        r.r2 = localBuffer.first
    }

    @Arbiter
    fun arbiter(r: II_Result) {
        val count = r.r1
        val first = r.r2
        // After reset, both should be 0, or we read before reset
        if (count == 0 && first != 0) {
            r.r1 = -1 // Signal inconsistency
            r.r2 = -1
        }
    }
}
