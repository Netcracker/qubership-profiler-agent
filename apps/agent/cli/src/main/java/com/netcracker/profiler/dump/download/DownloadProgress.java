package com.netcracker.profiler.dump.download;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class DownloadProgress {
    AtomicInteger filesDownloaded = new AtomicInteger();
    int filesTotal;
    AtomicLong bytesDownloaded = new AtomicLong();
    long bytesTotal;
    private volatile boolean isCompleted;
    volatile boolean isCanceled;
    List<Throwable> errors = Collections.synchronizedList(new ArrayList<>());

    public int getFilesDownloaded() {
        return filesDownloaded.get();
    }

    public int getFilesTotal() {
        return filesTotal;
    }

    public long getBytesDownloaded() {
        return bytesDownloaded.get();
    }

    public long getBytesTotal() {
        return bytesTotal;
    }

    public boolean isCompleted() {
        return isCompleted;
    }

    public List<Throwable> getErrors() {
        return errors;
    }

    public void waitUntilCompleted() {
        waitUntilCompleted(0L);
    }

    public synchronized void waitUntilCompleted(long timeoutMillis) {
        if (isCompleted) {
            return;
        }
        long deadline = timeoutMillis > 0 ? System.currentTimeMillis() + timeoutMillis : 0;
        try {
            while (!isCompleted) {
                if (timeoutMillis == 0) {
                    wait();
                } else {
                    long remaining = deadline - System.currentTimeMillis();
                    if (remaining <= 0) {
                        break;
                    }
                    wait(remaining);
                }
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    synchronized void setCompleted() {
        this.isCompleted = true;
        notifyAll();
    }

    public boolean isCanceled() {
        return isCanceled;
    }

    public void cancel() {
        isCanceled = true;
    }
}
