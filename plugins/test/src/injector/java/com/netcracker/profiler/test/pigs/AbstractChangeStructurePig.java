package com.netcracker.profiler.test.pigs;

public abstract class AbstractChangeStructurePig implements Runnable {
    protected String value;

    public String toString$profiler() {
        return value.toString();
    }
}
