package com.netcracker.profilerTest.testapp;

import com.netcracker.profiler.agent.Bootstrap;
import com.netcracker.profiler.agent.DumperConstants;
import com.netcracker.profiler.agent.DumperPlugin;
import com.netcracker.profiler.agent.DumperPlugin_07;
import com.netcracker.profiler.agent.LocalState;
import com.netcracker.profiler.agent.Profiler;
import com.netcracker.profiler.agent.ProfilerData;

import java.util.Random;

/**
 * A steady synthetic workload for the phase-2 generator calibration
 * (backend/docs/design/virtual-dumper.md §6): the reference "run A" the
 * virtual dumper's traffic profile is compared against.
 *
 * <p>Like {@link AdversarialMain}, it drives the agent through the
 * programmatic {@link Profiler} API instead of bytecode instrumentation, so
 * the do-not-profile E2E config applies unchanged and the recorded calls are
 * exactly the ones opened here. Every call is a root method with one nested
 * child (two enters, so the dumper records it regardless of duration), an
 * indexed {@code request.id} parameter with a unique value, and a duration
 * drawn from three classes; the class shares and rates are what the virtual
 * dumper mirrors with its knobs.
 *
 * <p>Args: {@code [seconds] [callsPerSecPerThread] [threads]}, defaulting to
 * {@code 120 5 3}.
 */
public final class LoadMain {

    private LoadMain() {
    }

    /** Duration classes: 90% short, 8% medium, 2% long (upper bounds in ms). */
    private static final double[] CLASS_SHARES = {0.90, 0.08, 0.02};
    private static final int[] CLASS_LO_MS = {1, 100, 1000};
    private static final int[] CLASS_HI_MS = {100, 1000, 3000};

    /** Distinct root methods per thread, bounding the dictionary size. */
    private static final int METHODS = 16;

    public static void main(String[] args) throws Exception {
        int seconds = args.length > 0 ? Integer.parseInt(args[0]) : 120;
        double callsPerSec = args.length > 1 ? Double.parseDouble(args[1]) : 5;
        int threads = args.length > 2 ? Integer.parseInt(args[2]) : 3;

        System.out.println("[load] " + threads + " threads x " + callsPerSec
                + " calls/s for " + seconds + " s");
        long deadline = System.currentTimeMillis() + seconds * 1000L;

        Thread[] workers = new Thread[threads];
        for (int i = 0; i < threads; i++) {
            final int id = i;
            workers[i] = new Thread(() -> runWorker(id, callsPerSec, deadline), "exec-" + i);
            workers[i].start();
        }
        for (Thread w : workers) {
            w.join();
        }

        System.out.println("[load] flushing profiler...");
        flushProfiler();
        System.out.println("[load] done.");
    }

    private static void runWorker(int id, double callsPerSec, long deadline) {
        Random rnd = new Random(1000L + id);
        long intervalMs = (long) (1000 / callsPerSec);
        int seq = 0;
        try {
            while (System.currentTimeMillis() < deadline) {
                long begin = System.currentTimeMillis();
                recordCall(rnd, id, seq++);
                long think = intervalMs - (System.currentTimeMillis() - begin);
                if (think > 0) {
                    Thread.sleep(think);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * One root call with a nested child carrying the sampled duration, plus an
     * indexed request.id value — the same shape the virtual dumper generates.
     */
    private static void recordCall(Random rnd, int thread, int seq) throws InterruptedException {
        int method = rnd.nextInt(METHODS);
        String methodName = "void com.load.gen.Svc" + String.format("%04d", thread)
                + ".op" + String.format("%02d", method) + "(int) (LoadMain.java) [test-app.jar]";
        int enterTag = ProfilerData.resolveTag(methodName) | DumperConstants.DATA_ENTER_RECORD;
        int childTag = ProfilerData.resolveTag(
                "void com.load.gen.Dao" + String.format("%04d", thread)
                        + ".query() (LoadMain.java) [test-app.jar]") | DumperConstants.DATA_ENTER_RECORD;

        LocalState state = Profiler.getState();
        state.enter(enterTag);
        try {
            state.event("req-load-" + thread + "-" + seq,
                    ProfilerData.resolveTag("request.id") | DumperConstants.DATA_TAG_RECORD);
            state.enter(childTag);
            try {
                Thread.sleep(sampleDurationMs(rnd));
            } finally {
                state.exit();
            }
        } finally {
            state.exit();
        }
    }

    private static int sampleDurationMs(Random rnd) {
        double r = rnd.nextDouble();
        int cls = CLASS_SHARES.length - 1;
        for (int i = 0; i < CLASS_SHARES.length; i++) {
            if (r < CLASS_SHARES[i]) {
                cls = i;
                break;
            }
            r -= CLASS_SHARES[i];
        }
        // Log-uniform inside the class, like the virtual dumper's sampler.
        double lo = Math.log(CLASS_LO_MS[cls]);
        double hi = Math.log(CLASS_HI_MS[cls]);
        return (int) Math.exp(lo + rnd.nextDouble() * (hi - lo));
    }

    /** Mirrors {@link AdversarialMain#flushProfiler()}. */
    private static void flushProfiler() throws InterruptedException {
        LocalState state = Profiler.getState();
        Profiler.exchangeBuffer(state.buffer);
        DumperPlugin dumper = Bootstrap.getPlugin(DumperPlugin.class);
        long timeoutMs = 15_000;
        if (dumper instanceof DumperPlugin_07) {
            ((DumperPlugin_07) dumper).gracefulShutdown(timeoutMs);
        } else {
            Thread.sleep(timeoutMs);
        }
    }
}
