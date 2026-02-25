package com.netcracker.profilerTest.testapp;

import com.netcracker.profiler.agent.*;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class Main {
    public static String test(String test) {
        // add non-trivial logic, so profiler instruments the method
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < test.length(); i++) {
            sb.append(test.charAt(i) | 0x20);
        }
        sb.setLength(0);
        for (int i = 0; i < test.length(); i++) {
            sb.append(test.charAt(i));
        }
        sb.setLength(0);
        for (int i = 0; i < test.length(); i++) {
            sb.append((char)(test.charAt(i) | 0x20));
        }
        return sb.toString();
    }

    /**
     * The actual profiled workload.  This method is large enough to be
     * instrumented by the profiler (unlike {@code main} which is kept
     * trivially small on purpose so the profiler skips it).
     *
     * <p>Because the profiler does not instrument {@code main}, this method
     * becomes the top-level "request" in the call tree.  It completes
     * normally, so by the time {@link #flushProfiler()} exchanges the
     * buffer the call is fully recorded (enter + exit).
     */
    public static void doWork(int iterations) throws InterruptedException {
        System.out.println("hello, world!");
        System.out.println("Waiting for profiler agent to initialize and send data...");
        for (int i = 0; i < iterations; i++) {
            test("Hello, world!");
            // Sleep so this call has visible duration (>1s) in the profiler
            Thread.sleep(TimeUnit.SECONDS.toMillis(1));
        }
    }

    /**
     * Flushes the current thread's profiler buffer and waits for the
     * dumper to write the data to disk.
     */
    public static void flushProfiler() throws InterruptedException {
        LocalState state = Profiler.getState();
        Profiler.exchangeBuffer(state.buffer);
        DumperPlugin dumper = Bootstrap.getPlugin(DumperPlugin.class);
        Duration timeout = Duration.ofSeconds(7);
        if (dumper instanceof DumperPlugin_07) {
            DumperPlugin_07 dumper7 = (DumperPlugin_07) dumper;
            // Give the dumper thread time to write the flushed data.
            // The dump interval defaults to 5s, so we need to wait at least that long.
            dumper7.gracefulShutdown(timeout.toMillis());
        } else {
            Thread.sleep(timeout.toMillis());
        }
    }

    // Keep main trivially small so the profiler does NOT instrument it.
    // This makes doWork the top-level profiled call.
    public static void main(String[] args) throws Exception {
        int iterations = args.length > 0 ? Integer.parseInt(args[0]) : 10;
        doWork(iterations);
        flushProfiler();
    }
}
