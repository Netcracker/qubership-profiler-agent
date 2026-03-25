package com.netcracker.cdt.ui.services.calls.tasks;

import io.quarkus.logging.Log;

import java.util.*;
import java.util.concurrent.*;

public final class MergeExecutor<K extends MergeExecutor.TaskWithPriority<V>, V> {
    private final ExecutorService executorService;

    private final List<Future<V>> futures = new LinkedList<>();
    private final Phaser workers = new Phaser(1); // phaser to check registered but un-arrived tasks

    public MergeExecutor(ExecutorService executorService) {
        this.executorService = executorService;
//        this.executorService = Executors.newFixedThreadPool(10, Thread.ofVirtual().factory());
//        Executors.newVirtualThreadPerTaskExecutor()
    }

    public void register(K originalTask) {
        var task = new Task<>(workers, originalTask);
        workers.register();
        var future = executorService.submit(task);
        futures.add(future);
    }

    public void waitNotTooLong(long timeoutMillis) {
        boolean needCancel = true;
        try {
            needCancel = !await(timeoutMillis);
        } finally {
            if (needCancel) {
                cancelRunning();
            }
        }
    }

    private boolean await(long timeoutMs) {
        Log.tracef("Waiting. Still %d pending tasks", workers.getUnarrivedParties());
        try {
            workers.awaitAdvanceInterruptibly(0, timeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        return workers.getUnarrivedParties() == 0;
    }

    public void cancelRunning() {
        int i = 0;
        for (var task: futures) {
            if (!task.isDone() && !task.isCancelled()) {
                task.cancel(true);
                i++;
            }
        }
        Log.infof("cancelled %d tasks", i);
//        // this is a safety feature to prevent tasks hanging in the thread pool due to improper handling of interruptions if any of the futures weren't cancelled
//        throw new IllegalStateException("failed to kill running threads. number remaining: " + numPending.get());
    }

    public static ExecutorService PriorityExecutor(int concurrency) {
        var queue = new PriorityBlockingQueue<>(concurrency * 10, byPriority());
        return new ThreadPoolExecutor(concurrency, concurrency, 0L, TimeUnit.MILLISECONDS, queue);
    }

    public interface TaskWithPriority<V> extends Callable<V> {
        long priority();
    }

    static Comparator<Runnable> byPriority() {
        return Comparator.<Runnable>comparingLong(t -> {
            if (t instanceof MergeExecutor.TaskWithPriority<?> pt) {
                return pt.priority();
            }
            return 0L;
        }).reversed();
    }

    // wrapper for TaskWithPriority in order to correctly mark tasks in phaser
    record Task<V>(Phaser workers, TaskWithPriority<V> original) implements TaskWithPriority<V> {
        @Override
        public long priority() {
            return original.priority();
        }

        @Override
        public V call()  {
            try {
                return original.call();
            } catch (Exception e) {
                Log.errorf(e,"problem with task [%d]", original.priority());
                throw new RuntimeException(e);
            } finally {
                workers.arriveAndDeregister();
            }
        }
    }

}

