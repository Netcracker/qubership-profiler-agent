package com.netcracker.profiler.agent;

import java.util.Arrays;
import java.util.concurrent.locks.LockSupport;

public class TimerCache extends Thread {
    private static final ESCLogger logger = ESCLogger.getLogger(TimerCache.class.getName());

    public final static TimerCache instance = new TimerCache();

    public volatile static long startTime = System.currentTimeMillis();
    // Record start time in nanoseconds - this allows future absolute time calculations based on difference of current
    // nanoTime with this timestamp
    public final static long startTimeNano = System.nanoTime();
    public volatile static long now = startTime;
    public volatile static int timer; // absolute time
    public volatile static long timerSHL32; // absolute time << 32
    public volatile static int timerWithoutSuspend; // time that does not include suspend events

    public static final int SUSPEND_LOG_SIZE = Integer.getInteger(TimerCache.class.getName() + ".SUSPEND_LOG_SIZE", 3600) - 1;
    public static final int MAX_TIMER_PAUSE = Integer.getInteger(TimerCache.class.getName() + ".MAX_TIMER_PAUSE", 50);
    public static final int MINIMAL_SUSPEND_TIME = Integer.getInteger(TimerCache.class.getName() + ".MINIMAL_SUSPEND_TIME", -1);
    // We assume to have 1 suspend event per second,
    // that is why a buffer of 3600 should be sufficient for a hour of work
    // Dumper should regularly flush the data, so it won't overwrite
    public static final long[] suspendDates = new long[SUSPEND_LOG_SIZE + 1];
    public static final int[] suspendDurations = new int[SUSPEND_LOG_SIZE + 1];
    public static volatile int lastSuspendEvent = 0, lastLoggedEvent = 0;

    public TimerCache() {
        super("Timer cache thread");
        setDaemon(true);
        setPriority(Thread.MAX_PRIORITY);
        start();
    }

    @Override
    public void run() {
        long startTime = TimerCache.startTime;
        int minimalSuspendTime = MINIMAL_SUSPEND_TIME;
        boolean suspendCalibrationRequired = false;
        if (minimalSuspendTime == -1) {
            suspendCalibrationRequired = true;
            minimalSuspendTime = 5;
        }
        final long[] dates = suspendDates;
        final int[] durations = suspendDurations;
        int prevTimer = 0;
        int pos = 0;
        long startWithSuspendOffset = startTime;
        int shortestObservedDeltaTime = MAX_TIMER_PAUSE;

        while (instance != null) {
            long now = System.currentTimeMillis();
            int timer = (int) (now - startTime);
            TimerCache.timer = timer;
            TimerCache.timerSHL32 = ((long) timer) << 32;
            TimerCache.now = now;
            int dt = timer - prevTimer;
            shortestObservedDeltaTime = Math.min(dt, shortestObservedDeltaTime);
            if (dt > minimalSuspendTime) {
                // Suspend detected
                startWithSuspendOffset += dt - shortestObservedDeltaTime;

                dates[pos] = now;
                durations[pos] = dt;
                lastSuspendEvent = pos = pos == SUSPEND_LOG_SIZE ? 0 : pos + 1;
            }
            TimerCache.timerWithoutSuspend = (int) (now - startWithSuspendOffset);
            if (suspendCalibrationRequired && (pos > 100 || timer > 10000)) {
                suspendCalibrationRequired = false;
                minimalSuspendTime = detectSuspendTime(minimalSuspendTime);
            }
            prevTimer = timer;
            LockSupport.parkNanos(800000);
        }
    }

    private int detectSuspendTime(int minimalSuspendTime) {
        int numGC = lastSuspendEvent;
        int totalTime = timer;
        int totalSuspend = timer - timerWithoutSuspend;
        if (totalSuspend * 5 < totalTime) {
            logger.fine("Profiler: all the durations exceeding " + minimalSuspendTime + "ms will be charged to suspension");
            return minimalSuspendTime; //
        }

        int[] dt = new int[numGC];
        System.arraycopy(suspendDurations, 0, dt, 0,
                Math.min(suspendDurations.length, dt.length));
        Arrays.sort(dt);

        for (numGC--; numGC >= 0 && dt[numGC] > MAX_TIMER_PAUSE; numGC--) {
            totalSuspend -= dt[numGC];
            totalTime -= dt[numGC];
        }

        int numBigPauses = dt.length - 1 - numGC;
        if (numGC == -1) {
            logger.fine("Profiler: all the suspension events lasted more than expected " + MAX_TIMER_PAUSE + "ms (MAX_TIMER_PAUSE). " +
                    "Assuming " + numBigPauses + " (all) of them " + (numBigPauses == 1 ? "is" : "are") + " related to abnormal pauses (the fastest was " + dt[numGC + 1] + "ms, the slowest was " + dt[dt.length - 1] + "ms).");
        } else {
            if (numGC < dt.length - 1) {
                logger.fine("Profiler: assuming pauses longer than " + MAX_TIMER_PAUSE + "ms (MAX_TIMER_PAUSE) are caused by gc/swap. " +
                        "Assuming " + numBigPauses + " of them " + (numBigPauses == 1 ? "is" : "are") + " related to abnormal pauses (the fastest was " + dt[numGC + 1] + "ms, the slowest was " + dt[dt.length - 1] + "ms).");
            }
            int i = 0;
            for (; totalSuspend * 20 > totalTime && i < numGC; ) {
                minimalSuspendTime = dt[i];
                for (; i < numGC && dt[i] <= minimalSuspendTime; i++) {
                    totalSuspend -= dt[i];
                }
            }
        }

        minimalSuspendTime *= 2;

        logger.fine("Profiler: all the durations exceeding " + minimalSuspendTime + "ms will be charged to suspension");
        return minimalSuspendTime;
    }

    public static void main(String[] args) throws InterruptedException {
        instance.getId();
        Thread.sleep(150000);
    }
}
