package com.netcracker.profiler.test.pigs;

public abstract class AbstractChangeStructurePig implements Runnable {
    protected String value;

    public AbstractChangeStructurePig(String value) {
        this.value = value;
    }
}
