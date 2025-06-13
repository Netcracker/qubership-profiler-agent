package org.qubership.profiler.reselience

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.parallel.Isolated
import org.qubership.profiler.agent.Profiler

@Isolated("Might trigger OOM and kill the other test by mistake")
class OutOfMemoryStoringEventsTest {
    @Test
    fun `add many large events works`() {
        val value = "a".repeat(1_000_000)
        Profiler.enter("OutOfMemoryStoringEventsTest.add many large events works")
        try {
            repeat(10000) {
                Profiler.enter("OutOfMemoryStoringEventsTest.small method")
                try {
                    // Generate a new object so it consumes RAM
                    val sb = StringBuilder(value)
                    Profiler.event(sb, "xml")
                } finally {
                    Profiler.exit();
                }
            }
        } finally {
            Profiler.exit();
        }
    }
}
