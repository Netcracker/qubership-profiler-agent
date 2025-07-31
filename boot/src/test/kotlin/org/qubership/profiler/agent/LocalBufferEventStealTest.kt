package org.qubership.profiler.agent

import org.junit.platform.commons.annotation.Testable
import org.openjdk.jcstress.annotations.*
import org.openjdk.jcstress.infra.results.IIL_Result
import org.openjdk.jcstress.infra.results.IJL_Result
import org.openjdk.jcstress.infra.results.IJ_Result
import org.openjdk.jcstress.infra.results.IL_Result

@JCStressTest
@Outcome(id = ["0, 0, null"], expect = Expect.ACCEPTABLE, desc = "Count field update was not visible")
@Outcome(id = ["1, 1, value"], expect = Expect.ACCEPTABLE, desc = "Count field update and event value was visible")
@Outcome(id = ["1, .*, null"], expect = Expect.FORBIDDEN, desc = "Event value should be visible if count is visible")
@Outcome(id = ["1, 0, .*"], expect = Expect.FORBIDDEN, desc = "Event tag should be visible if count is visible")
@State
@Testable
open class LocalBufferEventStealTest {

    private val localBuffer = LocalBuffer()

    @Actor
    fun writer() {
        localBuffer.event("value", 42)
    }

    @Actor
    fun reader(r: IIL_Result) {
        val count = localBuffer.count
        r.r1 = count
        if (count > 0) {
            r.r2 = if (localBuffer.data[0] == 0L) 0 else 1;
            r.r3 = localBuffer.value[0]
        }
    }
}
