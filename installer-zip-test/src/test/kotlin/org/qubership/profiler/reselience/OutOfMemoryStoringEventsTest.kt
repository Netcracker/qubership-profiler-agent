package org.qubership.profiler.reselience

import org.junit.jupiter.api.Test
import org.qubership.profiler.agent.Profiler

class OutOfMemoryStoringEventsTest {
    @Test
    fun `add many large events works`() {
        val value = "a".repeat(10_000_000)
        Profiler.enter("OutOfMemoryStoringEventsTest.add many large events works")
        try {
            repeat(1000) {
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
