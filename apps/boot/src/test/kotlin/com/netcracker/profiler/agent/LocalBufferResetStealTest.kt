package com.netcracker.profiler.agent

import org.junit.platform.commons.annotation.Testable
import org.openjdk.jcstress.annotations.*
import org.openjdk.jcstress.infra.results.IJ_Result

@JCStressTest
@Outcome(id = ["0, 0"], expect = Expect.ACCEPTABLE, desc = "Count field update was not visible or the buffer was reset")
@Outcome(id = ["1, 1311768467463790320"], expect = Expect.ACCEPTABLE, desc = "Count field update and enter event was visible")
@Outcome(id = ["1, 0"], expect = Expect.FORBIDDEN, desc = "Method enter event should be visible if count is visible")
@State
@Testable
open class LocalBufferResetStealTest {

    private val localBuffer = LocalBuffer()

    @Actor
    fun writer() {
        localBuffer.initTimedEnter(0x1234_5678_9abc_def0L)
        localBuffer.reset()
    }

    @Actor
    fun reader(r: IJ_Result) {
        val count = localBuffer.count
        r.r1 = count
        if (count > 0) {
            r.r2 = localBuffer.data[0]
        }
    }
}
