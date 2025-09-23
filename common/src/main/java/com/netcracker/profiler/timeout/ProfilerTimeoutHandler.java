package com.netcracker.profiler.timeout;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

public class ProfilerTimeoutHandler {
    private static Timer timeoutTimer = new Timer("ProfilerTimeoutTimer", true);
    private static ConcurrentHashMap<Thread, ProfilerTimeoutTask> timerTasksHash = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<Thread, Boolean> timeoutMap = new ConcurrentHashMap<>();

    private static final int TIMEOUT_DURATION = Integer.getInteger("com.netcracker.profiler.agent.Profiler.UI_TIMEOUT", 540000);
    private static final boolean TIMEOUT_ENABLED = TIMEOUT_DURATION == 0 ? false : true;

    public static ProfilerTimeoutTask scheduleTimeout() {
        return scheduleTimeout(TIMEOUT_DURATION, Thread.currentThread());
    }

    public static ProfilerTimeoutTask scheduleTimeout(int duration) {
        return scheduleTimeout(duration, Thread.currentThread());
    }

    public static ProfilerTimeoutTask scheduleTimeout(int duration, Thread thread) {
        if(!TIMEOUT_ENABLED) return null;
        if (timerTasksHash.get(thread) != null)
            throw new IllegalStateException("Interrupt task is already scheduled for the thread " + thread);
        if (duration <= 0)
            return null;
        ProfilerTimeoutTask profilerTimeoutTask = new ProfilerTimeoutTask(thread);
        timeoutTimer.schedule(profilerTimeoutTask, duration);
        timerTasksHash.put(thread, profilerTimeoutTask);
        return profilerTimeoutTask;
    }

    public static ProfilerTimeoutTask cancelTimeout() {
        return cancelTimeout(Thread.currentThread());
    }

    public static ProfilerTimeoutTask cancelTimeout(Thread thread) {
        if(!TIMEOUT_ENABLED) return null;
        timeoutMap.remove(thread);
        ProfilerTimeoutTask profilerTimeoutTask = timerTasksHash.remove(thread);
        if (profilerTimeoutTask != null) {
            profilerTimeoutTask.cancel();
            timeoutTimer.purge();
        }
        return profilerTimeoutTask;
    }

    public static void checkTimeout() throws ProfilerTimeoutException {
        if(TIMEOUT_ENABLED) {
            Boolean timeout = timeoutMap.get(Thread.currentThread());
            if(timeout != null && timeout) {
                throw new ProfilerTimeoutException(TIMEOUT_DURATION);
            }
        }
    }

    public static <T> T executeWithTimeout(Callable<T> callable) {
        scheduleTimeout();
        try {
            return callable.call();
        } catch (ProfilerTimeoutException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            cancelTimeout();
        }
    }

    public static class ProfilerTimeoutTask extends TimerTask {
        Thread thread = null;

        public ProfilerTimeoutTask(Thread thread) {
            this.thread = thread;
        }

        public void run() {
            timeoutMap.put(thread, true);
        }
    }
}
