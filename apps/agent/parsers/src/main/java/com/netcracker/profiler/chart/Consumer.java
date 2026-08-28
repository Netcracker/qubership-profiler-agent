package com.netcracker.profiler.chart;

public interface Consumer<T> {
    public void consume(T t);
}
