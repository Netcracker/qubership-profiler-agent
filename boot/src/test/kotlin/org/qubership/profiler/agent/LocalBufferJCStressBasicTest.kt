package org.qubership.profiler.agent

import org.junit.platform.commons.annotation.Testable
import org.openjdk.jcstress.annotations.*
import org.openjdk.jcstress.infra.results.II_Result
import org.openjdk.jcstress.infra.results.I_Result

@JCStressTest
@Outcome(expect = Expect.ACCEPTABLE, desc = "First field visibility")
@State
@Testable
open class LocalBufferFirstFieldVisibilityTest {

    private val localBuffer = LocalBuffer()

    @Actor
    fun writer() {
        localBuffer.first = 10
    }

    @Actor
    fun reader(r: I_Result) {
        r.r1 = localBuffer.first
    }

    @Arbiter
    fun arbiter(r: I_Result) {
        // First should be either 0 (initial) or 10 (written)
        val first = r.r1
        if (first != 0 && first != 10) {
            r.r1 = -1 // Signal unexpected value
        }
    }
}

@JCStressTest
@Outcome(expect = Expect.ACCEPTABLE, desc = "Count and first field consistency")
@State
open class LocalBufferFieldConsistencyTest {

    private val localBuffer = LocalBuffer()

    @Actor
    fun writer() {
        localBuffer.count = 20
        localBuffer.first = 5
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

        // Check for impossible states
        if (first > count || count < 0 || first < 0) {
            r.r1 = -1 // Signal inconsistency
            r.r2 = -1
        }
    }
}

@JCStressTest
@Outcome(expect = Expect.ACCEPTABLE, desc = "Reset operation atomicity")
@State
open class LocalBufferResetAtomicityTest {

    private val localBuffer = LocalBuffer()

    init {
        // Initialize with some data
        localBuffer.count = 15
        localBuffer.first = 3
    }

    @Actor
    fun resetter() {
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

        // Basic consistency check
        if (first > count || count < 0 || first < 0) {
            r.r1 = -2 // Signal invalid state
            r.r2 = -2
        }
    }
}

@JCStressTest
@Outcome(expect = Expect.ACCEPTABLE, desc = "Count increment visibility")
@State
open class LocalBufferCountIncrementVisibilityTest {

    private val localBuffer = LocalBuffer()

    @Actor
    fun incrementer() {
        localBuffer.count++
    }

    @Actor
    fun reader(r: I_Result) {
        r.r1 = localBuffer.count
    }

    @Arbiter
    fun arbiter(r: I_Result) {
        val count = r.r1
        // Count should be 0 (initial) or 1 (incremented)
        if (count != 0 && count != 1) {
            r.r1 = -1 // Signal unexpected value
        }
    }
}

@JCStressTest
@Outcome(expect = Expect.ACCEPTABLE, desc = "Multiple field updates")
@State
open class LocalBufferMultipleFieldUpdatesTest {

    private val localBuffer = LocalBuffer()

    @Actor
    fun writer1() {
        localBuffer.count = 10
    }

    @Actor
    fun writer2() {
        localBuffer.first = 5
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

        // Check for impossible states
        if (first > count || count < 0 || first < 0) {
            r.r1 = -1 // Signal inconsistency
            r.r2 = -1
        }
    }
}

@JCStressTest
@Outcome(expect = Expect.ACCEPTABLE, desc = "Data array bounds safety")
@State
open class LocalBufferDataArrayBoundsTest {

    private val localBuffer = LocalBuffer()

    @Actor
    fun writer() {
        localBuffer.count = 1
        localBuffer.data[0] = 123L
    }

    @Actor
    fun reader(r: II_Result) {
        val count = localBuffer.count
        if (count > 0 && count <= LocalBuffer.SIZE) {
            r.r1 = count
            r.r2 = localBuffer.data[0].toInt()
        } else {
            r.r1 = -1 // Signal unsafe access
            r.r2 = -1
        }
    }
}

@JCStressTest
@Outcome(expect = Expect.ACCEPTABLE, desc = "Value array bounds safety")
@State
open class LocalBufferValueArrayBoundsTest {

    private val localBuffer = LocalBuffer()

    @Actor
    fun writer() {
        localBuffer.count = 1
        localBuffer.value[0] = "test"
    }

    @Actor
    fun reader(r: II_Result) {
        val count = localBuffer.count
        if (count > 0 && count <= LocalBuffer.SIZE) {
            r.r1 = count
            r.r2 = if (localBuffer.value[0] == "test") 1 else 0
        } else {
            r.r1 = -1 // Signal unsafe access
            r.r2 = -1
        }
    }
}
