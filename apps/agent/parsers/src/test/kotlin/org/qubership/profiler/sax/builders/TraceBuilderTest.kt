package org.qubership.profiler.sax.builders

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import com.netcracker.profiler.io.Hotspot
import com.netcracker.profiler.io.SuspendLog
import com.netcracker.profiler.sax.builders.TreeBuilderTrace
import com.netcracker.profiler.sax.values.StringValue

class TraceBuilderTest {
    private fun assertChildren(root: Hotspot, string: String) {
        assertEquals(
            string,
            root.children.joinToString("\n") {
                mapOf(
                    "methodId" to it.id,
                    "duration" to it.totalTime,
                    "tags" to it.tags?.values?.joinToString(separator = ",\n    ", prefix = "[\n    ", postfix = "]"),
                ).toString()
            }
        )
    }

    @Test
    fun `multiple values in single method`() {
        val root = Hotspot(-1)
        TreeBuilderTrace(root, SuspendLog.EMPTY).apply {
            visitEnter(1)
            visitLabel(40, StringValue("select 1"))
            visitTimeAdvance(5)
            visitLabel(40, StringValue("select 2"))
            visitExit()
            visitEnd()
        }
        assertChildren(
            root,
            """
            {methodId=1, duration=5, tags=[
                HotspotTag{id=40, count=1, totalTime=5, values=[select 1, select 2]}]}
            """.trimIndent()
        )
    }

    @Test
    fun `same values aggregate in a single method`() {
        val root = Hotspot(-1)
        TreeBuilderTrace(root, SuspendLog.EMPTY).apply {
            repeat(5) {
                visitEnter(1)
                visitLabel(40, StringValue("select 1"))
                visitTimeAdvance(5)
                visitLabel(40, StringValue("select 2"))
                visitExit()
            }
            visitEnd()
        }
        assertChildren(
            root,
            """
            {methodId=1, duration=25, tags=[
                HotspotTag{id=40, count=5, totalTime=25, values=[select 1, select 2]}]}
            """.trimIndent()
        )
    }

    @Test
    fun `aggregation when queries are different`() {
        val root = Hotspot(-1)
        TreeBuilderTrace(root, SuspendLog.EMPTY).apply {
            visitEnter(1)
            visitLabel(40, StringValue("select 1"))
            visitTimeAdvance(5)
            visitLabel(40, StringValue("select 2"))
            visitExit()

            visitEnter(1)
            visitTimeAdvance(10)
            visitLabel(40, StringValue("select 3"))
            visitExit()

            visitEnd()
        }
        assertChildren(
            root,
            """
            {methodId=1, duration=15, tags=[
                HotspotTag{id=40, count=1, totalTime=5, values=[select 1, select 2]},
                HotspotTag{id=40, count=1, totalTime=10, values=[select 3]}]}
            """.trimIndent()
        )
    }

    @Test
    fun `different queries do not merge together`() {
        val root = Hotspot(-1)
        TreeBuilderTrace(root, SuspendLog.EMPTY).apply {
            visitEnter(1)
            visitLabel(40, StringValue("select 1"))
            visitTimeAdvance(5)
            visitLabel(40, StringValue("select 2"))
            visitExit()

            visitEnter(1)
            visitTimeAdvance(10)
            visitLabel(40, StringValue("select 3"))
            visitExit()

            visitEnd()
        }
        assertChildren(
            root,
            """
            {methodId=1, duration=15, tags=[
                HotspotTag{id=40, count=1, totalTime=5, values=[select 1, select 2]},
                HotspotTag{id=40, count=1, totalTime=10, values=[select 3]}]}
            """.trimIndent()
        )
    }

    @Test
    fun `queries aggregate across calls`() {
        val root = Hotspot(-1)
        TreeBuilderTrace(root, SuspendLog.EMPTY).apply {
            visitEnter(1)
            visitTimeAdvance(5)
            visitLabel(40, StringValue("select 1"))
            visitLabel(40, StringValue("select 2"))
            visitExit()

            visitEnter(1)
            visitTimeAdvance(10)
            visitLabel(40, StringValue("select 3"))
            visitExit()

            visitEnter(1)
            visitTimeAdvance(2)
            visitLabel(40, StringValue("select 1"))
            visitLabel(40, StringValue("select 2"))
            visitExit()

            visitEnter(1)
            visitTimeAdvance(3)
            visitLabel(40, StringValue("select 3"))
            visitExit()

            visitEnd()
        }
        assertChildren(
            root,
            """
            {methodId=1, duration=20, tags=[
                HotspotTag{id=40, count=2, totalTime=7, values=[select 1, select 2]},
                HotspotTag{id=40, count=2, totalTime=13, values=[select 3]}]}
            """.trimIndent()
        )
    }

    @Test
    fun `too many queries conflate to OTHER`() {
        val root = Hotspot(-1)
        TreeBuilderTrace(root, SuspendLog.EMPTY).apply {
            repeat(400) {
                visitEnter(1)
                visitTimeAdvance(1)
                visitLabel(40, StringValue("select 1"))
                visitLabel(40, StringValue("select $it"))
                visitExit()
            }
            visitEnd()
        }
        assertChildren(
            root,
            """
            {methodId=1, duration=400, tags=[
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 0]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 2]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 3]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 4]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 5]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 6]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 7]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 8]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 9]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 10]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 11]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 12]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 13]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 14]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 15]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 16]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 17]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 18]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 19]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 20]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 21]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 22]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 23]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 24]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 25]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 26]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 27]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 28]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 29]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 30]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 31]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 32]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 33]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 34]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 35]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 36]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 37]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 38]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 39]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 40]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 41]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 42]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 43]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 44]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 45]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 46]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 47]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 48]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 49]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 50]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 51]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 52]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 53]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 54]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 55]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 56]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 57]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 58]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 59]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 60]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 61]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 62]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 63]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 64]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 65]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 66]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 67]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 68]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 69]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 70]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 71]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 72]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 73]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 74]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 75]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 76]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 77]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 78]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 79]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 80]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 81]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 82]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 83]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 84]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 85]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 86]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 87]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 88]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 89]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 90]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 91]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 92]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 93]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 94]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 95]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 96]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 97]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 98]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 99]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 100]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 101]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 102]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 103]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 104]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 105]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 106]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 107]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 108]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 109]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 110]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 111]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 112]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 113]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 114]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 115]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 116]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 117]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 118]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 119]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 120]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 121]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 122]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 123]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 124]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 125]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 126]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 127]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 128]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 129]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 130]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 131]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 132]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 133]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 134]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 135]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 136]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 137]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 138]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 139]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 140]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 141]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 142]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 143]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 144]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 145]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 146]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 147]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 148]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 149]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 150]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 151]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 152]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 153]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 154]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 155]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 156]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 157]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 158]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 159]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 160]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 161]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 162]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 163]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 164]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 165]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 166]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 167]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 168]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 169]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 170]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 171]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 172]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 173]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 174]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 175]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 176]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 177]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 178]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 179]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 180]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 181]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 182]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 183]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 184]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 185]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 186]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 187]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 188]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 189]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 190]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 191]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 192]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 193]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 194]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 195]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 196]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 197]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 198]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 199]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 200]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 201]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 202]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 203]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 204]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 205]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 206]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 207]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 208]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 209]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 210]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 211]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 212]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 213]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 214]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 215]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 216]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 217]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 218]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 219]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 220]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 221]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 222]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 223]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 224]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 225]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 226]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 227]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 228]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 229]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 230]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 231]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 232]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 233]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 234]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 235]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 236]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 237]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 238]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 239]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 240]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 241]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 242]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 243]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 244]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 245]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 246]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 247]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 248]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 249]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 250]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 251]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 252]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 253]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 254]},
                HotspotTag{id=40, count=1, totalTime=1, values=[select 1, select 255]},
                HotspotTag{id=40, count=144, totalTime=144, values=[::other]}]}
            """.trimIndent()
        )
    }
}
