package com.netcracker.profiler.agent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * Verifies that application threads keep running when the dumper never starts and
 * {@code BLOCK_WHEN_DIRTY_BUFFERS_QUEUE_IS_FULL} is enabled.
 *
 * <p>Each case runs in a child JVM: {@link Profiler} starts the dumper in its static initializer,
 * and the queue limits are static final fields read from system properties, so neither can be
 * set up twice in one JVM.
 */
class ProfilerDumperStartupFailureTest {
    private static final String THROWING_DUMPER = "throwing-dumper";
    private static final String MISSING_DUMPER = "missing-dumper";
    // Enough calls to fill the two-buffer queue many times over.
    private static final int CALLS = 10000;
    private static final int PROBE_TIMEOUT_SECONDS = 60;

    @TempDir
    Path tempDir;

    @Test
    void applicationCallsContinue_whenDumperCreationThrows() throws Exception {
        assertProbeCompletes(THROWING_DUMPER);
    }

    @Test
    void applicationCallsContinue_whenDumperIsMissing() throws Exception {
        assertProbeCompletes(MISSING_DUMPER);
    }

    private void assertProbeCompletes(String scenario) throws Exception {
        File output = tempDir.resolve(scenario + ".log").toFile();
        List<String> command = new ArrayList<String>();
        command.add(new File(System.getProperty("java.home"), "bin/java").getPath());
        command.add("-D" + Profiler.class.getName() + ".BLOCK_WHEN_DIRTY_BUFFERS_QUEUE_IS_FULL=true");
        command.add("-D" + Profiler.class.getName() + ".INITIAL_BUFFERS=2");
        command.add("-D" + Profiler.class.getName() + ".MAX_BUFFERS=2");
        command.add("-D" + LocalBuffer.class.getName() + ".SIZE=16");
        // Short calls are otherwise logged lazily and never reach the dirty buffers queue.
        command.add("-D" + Profiler.class.getName() + ".minimal_logged_duration=0");
        command.add(Probe.class.getName());
        command.add(scenario);
        ProcessBuilder builder = new ProcessBuilder(command);
        // The class path goes through the environment: it can exceed the command-line limit on Windows.
        builder.environment().put("CLASSPATH", System.getProperty("java.class.path"));
        builder.redirectErrorStream(true);
        builder.redirectOutput(output);

        Process probe = builder.start();
        boolean finished;
        try {
            finished = probe.waitFor(PROBE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } finally {
            probe.destroyForcibly();
        }
        String log = new String(Files.readAllBytes(output.toPath()), StandardCharsets.UTF_8);
        assertTrue(finished, "The application thread should not block when the dumper is not started."
                + " Probe output:\n" + log);
        assertEquals(0, probe.waitFor(), "Probe output:\n" + log);
    }

    /** Plays the application: makes instrumented calls after the dumper failed to start. */
    static class Probe {
        public static void main(String[] args) {
            if (THROWING_DUMPER.equals(args[0])) {
                Bootstrap.registerPlugin(DumperPlugin.class, new ThrowingDumper());
            }
            for (int i = 0; i < CALLS; i++) {
                Profiler.enter("void " + Probe.class.getName() + ".call() (Probe.java:1) [test.jar]");
                Profiler.exit();
            }
        }
    }

    static class ThrowingDumper implements DumperPlugin_02 {
        @Override
        public void newDumper(BlockingQueue<LocalBuffer> dirtyBuffers, BlockingQueue<LocalBuffer> emptyBuffers,
                ArrayList<LocalBuffer> buffers) {
            throw new UnsupportedOperationException("Unsupported operation");
        }

        @Override
        public void newDumper(BlockingQueue<LocalBuffer> dirtyBuffers, BlockingQueue<LocalBuffer> emptyBuffers,
                ConcurrentMap<Thread, LocalState> activeThreads) {
            throw new IllegalStateException("The dumper fails before it starts its consumer");
        }

        @Override
        public void reconfigure() {
        }

        @Override
        public File getCurrentRoot() {
            return null;
        }

        @Override
        public List<String> getTags() {
            return ProfilerData.getTags();
        }

        @Override
        public boolean start() {
            return false;
        }

        @Override
        public boolean stop(boolean force) {
            return false;
        }

        @Override
        public boolean isStarted() {
            return false;
        }

        @Override
        public int getNumberOfRestarts() {
            return 0;
        }

        @Override
        public long getWrittenRecords() {
            return 0;
        }

        @Override
        public long getWrittenBytes() {
            return 0;
        }

        @Override
        public long getWriteTime() {
            return 0;
        }

        @Override
        public long getDumperStartTime() {
            return 0;
        }
    }
}
