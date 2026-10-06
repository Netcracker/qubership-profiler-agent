package com.netcracker.profiler.io;

public class SuspendLogPair<K,V> {

    private K dateOfSuspend;
    private V delay;


    public SuspendLogPair() {
    }

    public SuspendLogPair(K dateOfSuspend, V delay) {
        this.dateOfSuspend = dateOfSuspend;
        this.delay = delay;
    }

    public K getDateOfSuspend() {
        return dateOfSuspend;
    }

    public V getDelay() {
        return delay;
    }
}
