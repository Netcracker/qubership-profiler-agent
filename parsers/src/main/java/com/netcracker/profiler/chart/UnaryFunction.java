package com.netcracker.profiler.chart;

public interface UnaryFunction<A, T> {
    public T evaluate(A arg);
}
