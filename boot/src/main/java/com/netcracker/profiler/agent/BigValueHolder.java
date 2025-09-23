package com.netcracker.profiler.agent;

public class BigValueHolder {
    private int index = -1;
    private int offset;
    protected Object value;

    public BigValueHolder(Object value) {
        this.value = value;
    }

    public synchronized void setAddress(int index, int offset) {
        this.index = index;
        this.offset = offset;
    }

    public Object getValue() {
        return value;
    }

    public synchronized int getIndex() {
        return index;
    }

    public synchronized int getOffset() {
        return offset;
    }
}
