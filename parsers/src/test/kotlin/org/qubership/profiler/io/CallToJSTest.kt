package org.qubership.profiler.io

import com.netcracker.profiler.configuration.ParameterInfoDto
import com.netcracker.profiler.io.Call
import com.netcracker.profiler.io.CallToJS
import com.netcracker.profiler.tags.DictionaryList
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.fail
import java.io.PrintWriter
import java.io.StringWriter
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.*
import java.util.regex.Pattern
import kotlin.io.path.readText

/**
 * Verifies that the field positions emitted by [com.netcracker.profiler.io.CallToJS.printCalls]
 * match the `C_*` constants defined in `ESCConstants.mjs`.
 *
 * If a field is added/removed/reordered in Java without updating the JS
 * constants (or vice-versa), this test will fail.
 */
class CallToJSTest {
    @Test
    fun printCallFieldOrderMatchesESCConstants() {
        val jsConstants = parseESCConstants()

        // Verify every entry in EXPECTED_FIELD_ORDER has the right index in ESCConstants
        for (i in EXPECTED_FIELD_ORDER.indices) {
            val name: String = EXPECTED_FIELD_ORDER[i]
            val jsIndex =
                jsConstants[name] ?: fail("ESCConstants.mjs is missing constant: $name (expected at index $i)")

            assertEquals(i, jsIndex) {
                "$name should be at index $i in ESCConstants.mjs"
            }
        }

        // Verify ESCConstants has no extra C_* constants beyond what we expect
        for (entry in jsConstants.entries) {
            val name = entry.key
            val idx = entry.value
            if (idx < EXPECTED_FIELD_ORDER.size) {
                assertEquals(name, EXPECTED_FIELD_ORDER[idx]) {
                    "ESCConstants.mjs index $idx has unexpected constant $name"
                }
            }
            // Indices >= EXPECTED_FIELD_ORDER.length are frontend-only (C_TITLE_HTML, etc.) -- OK
        }
    }

    @Test
    fun printCallOutputPositionsMatchSentinels() {
        // Use distinct primes so each field value is unique
        val call = Call()
        call.time = 1009
        call.queueWaitDuration = 103
        call.duration = 211
        call.cpuTime = 307
        call.suspendDuration = 401
        call.calls = 503
        call.method = 601
        call.transactions = 701
        call.memoryUsed = 809
        call.logsGenerated = 907
        call.logsWritten = 1013
        call.fileRead = 1103
        call.fileWritten = 1201
        call.netRead = 1301
        call.netWritten = 1409
        call.threadName = "test-thread"
        call.traceFileIndex = 2
        call.bufferOffset = 3
        call.recordIndex = 5

        // Give the call a parameter so C_PARAMS position is populated
        call.params = HashMap()
        call.params[42] = mutableListOf<String?>("sentinel_param")

        val sw = StringWriter()
        val pw = PrintWriter(sw)
        // rootFile=null causes printAdditionalRootReferenceDetails to log warning and return
        val callToJS = CallToJS(pw, null, null)

        val tags = DictionaryList(
            arrayOfNulls<String>(602).toMutableList()
        )
        // Set some tag values so they don't NPE
        val tagList = tags.asList()
        tagList[601] = "com.example.MyClass.myMethod"
        tagList[42] = "my.param"

        val requiredIds = BitSet()
        requiredIds.set(42)
        requiredIds.set(601)

        val paramInfo: MutableMap<String?, ParameterInfoDto?> = LinkedHashMap<String?, ParameterInfoDto?>()
        val pInfo = ParameterInfoDto("my.param")
        paramInfo["my.param"] = pInfo

        val calls = ArrayList<Call?>()
        calls.add(call)

        callToJS.printCalls("test/root", calls, tags, paramInfo, requiredIds)
        pw.flush()
        val output = sw.toString()

        // Extract the JS array from CL.append([...]); -- find the call array line
        // The array line looks like: [906,314,307,103,401,503,q,q+'_2_3_5',601,701,809,907,1013,2304,1201,2710,1409,{...}]
        var arrayMatcher = Pattern.compile("""\[([^\[\]]*\{[^}]*}[^\[\]]*)]""").matcher(output)
        if (!arrayMatcher.find()) {
            // Try without params object (shouldn't happen here, but fallback)
            arrayMatcher = Pattern.compile("""CL\.append\(\[\s*\[(.+)]""", Pattern.DOTALL).matcher(output)
            if (!arrayMatcher.find()) {
                fail("Could not find call array in output:\n$output")
            }
        }
        val arrayContent = arrayMatcher.group(1)

        // Split by comma, but respect nested {} for params
        val elements = splitArrayElements(arrayContent)

        val jsConstants = parseESCConstants()

        // Verify numeric fields by their sentinel values
        // Position 0: C_TIME = time - queueWaitDuration = 1009 - 103 = 906
        assertElement(elements, jsConstants, "906", "C_TIME")
        // Position 1: C_DURATION = duration + queueWaitDuration = 211 + 103 = 314
        assertElement(elements, jsConstants, "314", "C_DURATION")
        // Position 2: C_CPU_TIME = cpuTime = 307
        assertElement(elements, jsConstants, "307", "C_CPU_TIME")
        // Position 3: C_QUEUE_WAIT_TIME = queueWaitDuration = 103
        assertElement(elements, jsConstants, "103", "C_QUEUE_WAIT_TIME")
        // Position 4: C_SUSPENSION = suspendDuration = 401
        assertElement(elements, jsConstants, "401", "C_SUSPENSION")
        // Position 5: C_CALLS = calls = 503
        assertElement(elements, jsConstants, "503", "C_CALLS")
        // Position 6: C_FOLDER_ID = q (JS variable)
        assertElement(elements, jsConstants, "q", "C_FOLDER_ID")
        // Position 7: C_ROWID = q+'_2_3_5'
        assertEquals("q+'_2_3_5'", elements[jsConstants.getValue("C_ROWID")].trim { it <= ' ' }) {
            "C_ROWID should contain row id expression"
        }
        // Position 8: C_METHOD = method = 601
        assertElement(elements, jsConstants, "601", "C_METHOD")
        // Position 9: C_TRANSACTIONS = transactions = 701
        assertElement(elements, jsConstants, "701", "C_TRANSACTIONS")
        // Position 10: C_MEMORY_ALLOCATED = memoryUsed = 809
        assertElement(elements, jsConstants, "809", "C_MEMORY_ALLOCATED")
        // Position 11: C_LOG_GENERATED = logsGenerated = 907
        assertElement(elements, jsConstants, "907", "C_LOG_GENERATED")
        // Position 12: C_LOG_WRITTEN = logsWritten = 1013
        assertElement(elements, jsConstants, "1013", "C_LOG_WRITTEN")
        // Position 13: C_FILE_TOTAL = fileRead + fileWritten = 1103 + 1201 = 2304
        assertElement(elements, jsConstants, "2304", "C_FILE_TOTAL")
        // Position 14: C_FILE_WRITTEN = fileWritten = 1201
        assertElement(elements, jsConstants, "1201", "C_FILE_WRITTEN")
        // Position 15: C_NET_TOTAL = netRead + netWritten = 1301 + 1409 = 2710
        assertElement(elements, jsConstants, "2710", "C_NET_TOTAL")
        // Position 16: C_NET_WRITTEN = netWritten = 1409
        assertElement(elements, jsConstants, "1409", "C_NET_WRITTEN")
        // Position 17: C_PARAMS = {params object} -- just verify it starts with {
        val params = jsConstants.getValue("C_PARAMS")
        val paramsElement = elements[params].trim { it <= ' ' }
        if (!paramsElement.startsWith("{")) {
            fail(
                "C_PARAMS at position $params should be params object, got: $paramsElement"
            )
        }
    }

    companion object {
        /**
         * The expected field order in the JS array produced by `printCall`.
         * Each entry is the ESCConstants name for that array position.
         * Positions 0..16 are emitted unconditionally; position 17 (C_PARAMS) is
         * the optional params object.
         */
        private val EXPECTED_FIELD_ORDER = listOf(
            "C_TIME",  // 0: call.time - call.queueWaitDuration
            "C_DURATION",  // 1: call.duration + call.queueWaitDuration
            "C_CPU_TIME",  // 2: call.cpuTime
            "C_QUEUE_WAIT_TIME",  // 3: call.queueWaitDuration
            "C_SUSPENSION",  // 4: call.suspendDuration
            "C_CALLS",  // 5: call.calls
            "C_FOLDER_ID",  // 6: q (folder id variable)
            "C_ROWID",  // 7: rowId expression
            "C_METHOD",  // 8: call.method
            "C_TRANSACTIONS",  // 9: call.transactions
            "C_MEMORY_ALLOCATED",  // 10: call.memoryUsed
            "C_LOG_GENERATED",  // 11: call.logsGenerated
            "C_LOG_WRITTEN",  // 12: call.logsWritten
            "C_FILE_TOTAL",  // 13: call.fileRead + call.fileWritten
            "C_FILE_WRITTEN",  // 14: call.fileWritten
            "C_NET_TOTAL",  // 15: call.netRead + call.netWritten
            "C_NET_WRITTEN",  // 16: call.netWritten
            "C_PARAMS",  // 17: {params object}
        )

        private fun assertElement(elements: List<String>, jsPositions: Map<String, Int>, expected: String, name: String) {
            val index = jsPositions[name] ?: fail("ESCConstants.mjs is missing constant: $name")
            assertEquals(expected, elements[index].trim { it <= ' ' }) {
                "$name at position $index"
            }
        }

        /**
         * Split a JS array literal by top-level commas, keeping nested `{...}` intact.
         */
        private fun splitArrayElements(arrayContent: String): List<String> {
            val result = mutableListOf<String>()
            var depth = 0
            var start = 0
            for (i in 0..<arrayContent.length) {
                val c = arrayContent.get(i)
                when (c) {
                    '{' -> depth++
                    '}' -> depth--
                    ',' if depth == 0 -> {
                        result.add(arrayContent.substring(start, i))
                        start = i + 1
                    }
                }
            }
            result.add(arrayContent.substring(start))
            return result
        }

        /**
         * Parse `C_*` constants from `ESCConstants.mjs`, returning name-to-index map.
         */
        private fun parseESCConstants(): Map<String, Int> {
            val esPath = findESCConstantsPath()
            val content = esPath.readText()
            val constants = mutableMapOf<String, Int>()
            // Match lines like "    C_TIME: 0,"
            val p = Pattern.compile("(C_\\w+):\\s*(\\d+)")
            val m = p.matcher(content)
            while (m.find()) {
                constants[m.group(1)] = m.group(2).toInt()
            }
            if (constants.isEmpty()) {
                fail("No C_* constants found in $esPath")
            }
            return constants
        }

        private fun findESCConstantsPath(): Path {
            // Walk up from working directory to find project root with profiler-ui/
            var dir = Paths.get("").toAbsolutePath()
            for (i in 0..4) {
                val candidate = dir.resolve("profiler-ui/src/ESCConstants.mjs")
                if (Files.exists(candidate)) {
                    return candidate
                }
                dir = dir.parent
                if (dir == null) {
                    break
                }
            }
            // Fallback: try relative path from typical Gradle test working dir
            return Paths.get("../profiler-ui/src/ESCConstants.mjs")
        }
    }
}
