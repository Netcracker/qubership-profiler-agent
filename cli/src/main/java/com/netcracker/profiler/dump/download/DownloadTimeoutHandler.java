package com.netcracker.profiler.dump.download;

import java.io.InterruptedIOException;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

public class DownloadTimeoutHandler {
    private static final Timer timeoutTimer = new Timer("DownloadTimeoutHandler", true);
    private static final Map<Thread, DownloadTimeoutTask> timerTasksHash = new ConcurrentHashMap<>();

    public static DownloadTimeoutTask scheduleTimeout(int duration) {
        Thread thread = Thread.currentThread();
        if (timerTasksHash.get(thread) != null)
            throw new IllegalStateException("Interrupt task is already scheduled for the thread " + thread);
        if (duration <= 0)
            return null;
        DownloadTimeoutTask profilerTimeoutTask = new DownloadTimeoutTask(thread);
        timeoutTimer.schedule(profilerTimeoutTask, duration);
        timerTasksHash.put(thread, profilerTimeoutTask);
        return profilerTimeoutTask;
    }

    public static DownloadTimeoutTask cancelTimeout() {
        Thread thread = Thread.currentThread();
        DownloadTimeoutTask profilerTimeoutTask = timerTasksHash.remove(thread);
        if (profilerTimeoutTask != null) {
            profilerTimeoutTask.cancel();
            timeoutTimer.purge();
            Thread.interrupted(); //Clear interrupted flag;
        }
        return profilerTimeoutTask;
    }

    public static <T> T executeWithTimeout(Callable<T> callable, int timeout) throws TimeoutException {
        scheduleTimeout(timeout);
        try {
            return callable.call();
        } catch (InterruptedException | InterruptedIOException e) {
            throw new TimeoutException();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            cancelTimeout();
        }
    }

    public static class DownloadTimeoutTask extends TimerTask {
        Thread thread = null;

        public DownloadTimeoutTask(Thread thread) {
            this.thread = thread;
        }

        public void run() {
            thread.interrupt();
        }
    }
}
