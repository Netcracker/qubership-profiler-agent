package com.netcracker.profiler.io

import io.mockk.every
import io.mockk.mockk
import jakarta.servlet.http.HttpServletRequest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class TemporalUtilsTest {
    @Test
    fun defaultTimerangeUsesNow() {
        val request = mockk<HttpServletRequest> {
            every { getParameter(any()) } returns null
        }

        val before = System.currentTimeMillis()
        val temporal = TemporalUtils.parseTemporal(request)
        val after = System.currentTimeMillis()
        assertTrue(temporal.timerangeTo in before..after)
        assertEquals(temporal.timerangeTo - 15 * 60 * 1000, temporal.timerangeFrom)
        assertEquals(temporal.timerangeTo, temporal.serverUTC)
        assertEquals(1, temporal.autoUpdate)
        assertEquals(500, temporal.durationFrom)
        assertEquals(Long.MAX_VALUE, temporal.durationTo)
    }

    @Test
    fun last2MinTimerange() {
        val request = mockk<HttpServletRequest> {
            every { getParameter(any()) } returns null
            every { getParameter("last2MinOrRange") } returns "last2min"
        }

        val temporal = TemporalUtils.parseTemporal(request)

        assertEquals((3 * 60 * 1000).toLong(), temporal.timerangeTo - temporal.timerangeFrom)
    }

    @Test
    fun explicitTimerangeOverridesDefault() {
        val request = mockk<HttpServletRequest> {
            every { getParameter(any()) } returns null
            every { getParameter("timerange[min]") } returns "10"
            every { getParameter("timerange[max]") } returns "20"
            every { getParameter("timerange[autoUpdate]") } returns "0"
            every { getParameter("duration[min]") } returns "0"
            every { getParameter("duration[max]") } returns "123"
        }

        val temporal = TemporalUtils.parseTemporal(request)

        assertEquals(10, temporal.timerangeFrom)
        assertEquals(20, temporal.timerangeTo)
        assertEquals(0, temporal.autoUpdate)
        assertEquals(0, temporal.durationFrom)
        assertEquals(123, temporal.durationTo)
    }
}
