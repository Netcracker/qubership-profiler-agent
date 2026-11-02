package com.netcracker.profiler.test.spring

/** Number of lines kept after a matching line, enough for the stack trace that follows a plugin exception. */
private const val CONTEXT_LINES = 20

private val FAILURE_PATTERNS = listOf(
    // ProfilingTransformer: the class matched a rule and its instrumentation failed
    Regex("""Unable to instrument class """),
    // ProfilerPluginLoggerImpl.pluginError: a hook threw at run time
    Regex("""Exception in profiler plugin"""),
    // ProfilingTransformer: a rule names an if-enhancer filter that does not exist
    Regex("""Filter \S+ is not found"""),
    // ClassInfoImpl: the jar of a hooked class has no readable manifest, so a version-gated enhancer skips it
    Regex("""Unable to open jar file """),
)

/**
 * Returns the lines of the agent log that report an instrumentation or plugin failure, each followed by the next
 * [CONTEXT_LINES] lines of the log.
 *
 * The agent logs these messages only for a class that matched a rule, so every returned line concerns a hooked
 * class. Returns an empty list for a log with no such line.
 */
fun agentFailureLines(log: String): List<String> {
    val lines = log.lines()
    val result = mutableListOf<String>()
    var keepUntil = -1
    for ((index, line) in lines.withIndex()) {
        if (FAILURE_PATTERNS.any { it.containsMatchIn(line) }) {
            keepUntil = maxOf(keepUntil, index + CONTEXT_LINES)
        }
        if (index <= keepUntil) {
            result += line
        }
    }
    return result
}
