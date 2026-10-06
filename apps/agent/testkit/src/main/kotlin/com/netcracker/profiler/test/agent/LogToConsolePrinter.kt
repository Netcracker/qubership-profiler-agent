package com.netcracker.profiler.test.agent

import org.testcontainers.containers.output.OutputFrame
import java.util.function.Consumer

/**
 * Forwards Testcontainers output to the console, so it is easier to debug tests.
 */
class LogToConsolePrinter(private val prefix: String) : Consumer<OutputFrame?> {
    override fun accept(outputFrame: OutputFrame?) {
        val message = outputFrame?.utf8String
        if (message.isNullOrEmpty()) {
            return
        }
        if (outputFrame.type === OutputFrame.OutputType.STDERR) {
            System.err.print(prefix)
            System.err.print(message)
            return
        }
        print(prefix)
        print(message)
    }
}
