package com.netcracker.profiler.agent

import io.mockk.every
import io.mockk.junit5.MockKExtension
import io.mockk.mockkStatic
import io.mockk.spyk
import io.mockk.verify
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.extension.ExtendWith

@ExtendWith(MockKExtension::class)
class EventMemoryLimitTest {
    lateinit var state: LocalState
    var largeEventVolumeBefore: Long = 0

    @BeforeEach
    fun setup() {
        mockkStatic(Bootstrap::class)
        mockkStatic(Profiler::class)
        mockkStatic(ProfilerData::class)
        every { Bootstrap.getPlugin(any<Class<*>>()) } returns null
        state = createState()
        largeEventVolumeBefore = ProfilerData.largeEventsVolume.get()
    }

    @AfterEach
    fun teardown() {
        state.buffer.reset()
        assertEquals(
            largeEventVolumeBefore,
            ProfilerData.largeEventsVolume.get(),
            "Large event volume should be reset after each test"
        )
    }

    private fun createState(): LocalState {
        val buffer = LocalBuffer()
        val state = spyk<LocalState>()
        state.buffer = buffer
        buffer.state = state
        every { Profiler.getState() } returns state
        return state
    }

    @Test
    fun `small event does not request global counter`() {
        Profiler.event("select * from foo", "sql")
        verify(exactly = 0) {
            ProfilerData.reserveLargeEventVolume(any())
        }
    }

    @Test
    fun `medium event does not request global counter`() {
        Profiler.event(" ".repeat(10_000), "sql")
        verify(exactly = 0) {
            ProfilerData.reserveLargeEventVolume(any())
        }
    }

    @Test
    fun `largeEvent truncates when global counter is full`() {
        val volume = ProfilerData.EVENT_HEAP_THRESHOLD_BYTES - ProfilerData.largeEventsVolume.get()
        ProfilerData.reserveLargeEventVolume(volume)
        verify {
            ProfilerData.reserveLargeEventVolume(volume)
        }
        try {
            // Log a large event and assert it is truncated
            val len = 1_000_000L
            Profiler.event(" ".repeat(len.toInt()), "sql")
            assertDoesNotThrow("Global counter is depleted, so ProfilerData.reserveLargeEventVolume should return false") {
                verify(exactly = 1) {
                    assertFalse(
                        ProfilerData.reserveLargeEventVolume(len),
                        "Global counter is depleted, so ProfilerData.reserveLargeEventVolume should return false"
                    )
                }
            }
            val value: Any? = state.buffer.value[0]
            val abbreviated = value.toString().replace(Regex("^ +")) { "[' ' repeated ${it.value.length} times]" }
            assertEquals(
                "[' ' repeated ${ProfilerData.TRUNCATED_EVENTS_THRESHOLD} times]... truncated from $len chars",
                abbreviated
            )
        } finally {
            ProfilerData.reserveLargeEventVolume(-volume)
        }
    }

    @Test
    fun `largeEvent reservation from global counter`() {
        Profiler.event(" ".repeat(100000), "sql")

        assertDoesNotThrow("Logging 100K reserves LocalState counter") {
            verify(exactly = 1) {
                state.reserveLargeEventVolume(100_000)
            }
        }

        assertDoesNotThrow("Logging 100K does not reserve global counter") {
            verify(exactly = 0) {
                ProfilerData.reserveLargeEventVolume(any())
            }
        }

        Profiler.event(" ".repeat(110_000), "sql")

        assertDoesNotThrow("Logging 110K reserves LocalState counter") {
            verify(exactly = 1) {
                state.reserveLargeEventVolume(110_000)
            }
        }

        assertDoesNotThrow("Logging 100K+110K should not reserve global counter") {
            verify(exactly = 0) {
                ProfilerData.reserveLargeEventVolume(any())
            }
        }

        Profiler.event(" ".repeat(120_000), "sql")

        assertDoesNotThrow("Logging 120K reserves LocalState counter") {
            verify(exactly = 1) {
                state.reserveLargeEventVolume(120_000)
            }
        }

        assertDoesNotThrow("Logging 100K+110K+120K should reserve for 330K from the global counter") {
            verify(exactly = 1) {
                ProfilerData.reserveLargeEventVolume(330_000)
            }
        }
    }
}
