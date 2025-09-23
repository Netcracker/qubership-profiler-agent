package com.netcracker.profiler.test.agent;

import org.testcontainers.containers.output.OutputFrame;

import java.util.function.Consumer;

/**
 * Forwards Testcontainers output to the console, so it is easier to debug tests.
 */
public final class LogToConsolePrinter implements Consumer<OutputFrame> {
    private final String prefix;

    public LogToConsolePrinter(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public void accept(OutputFrame outputFrame) {
        String message = outputFrame.getUtf8String();
        if (message.isEmpty()) {
            return;
        }
        if (outputFrame.getType() == OutputFrame.OutputType.STDERR) {
            System.err.print(prefix);
            System.err.print(message);
            return;
        }
        System.out.print(prefix);
        System.out.print(message);
    }
}
