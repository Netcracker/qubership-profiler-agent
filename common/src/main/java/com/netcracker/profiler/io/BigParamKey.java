package com.netcracker.profiler.io;

public class BigParamKey {
    final int traceIndex;
    final int offset;

    public BigParamKey(int traceIndex, int offset) {
        this.traceIndex = traceIndex;
        this.offset = offset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BigParamKey that = (BigParamKey) o;

        if (offset != that.offset) return false;
        if (traceIndex != that.traceIndex) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = traceIndex;
        result = 31 * result + offset;
        return result;
    }
}
