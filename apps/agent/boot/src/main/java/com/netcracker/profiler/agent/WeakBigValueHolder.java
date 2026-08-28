package com.netcracker.profiler.agent;

public class WeakBigValueHolder extends BigValueHolder {
    public WeakBigValueHolder(Object value) {
        super(value);
    }

    @Override
    public synchronized void setAddress(int index, int offset) {
        super.setAddress(index, offset);
        value = null;
    }
}
