package com.netcracker.profiler.io;

import java.util.Arrays;

public class ArraySuspendLogCollection implements ISuspendLogCollection {

    private long[] dates;
    private int[] delays;
    private int[] trueDelays;
    private int size;

    public ArraySuspendLogCollection(long[] dates, int[] delays, int[] trueDelays) {
        this(dates, delays, trueDelays, dates.length);
    }

    public ArraySuspendLogCollection(long[] dates, int[] delays, int[] trueDelays, int size) {
        this.dates = dates;
        this.delays = delays;
        this.trueDelays = trueDelays;
        this.size = size;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public long getDate(int index) {
        return dates[index];
    }

    @Override
    public int getDelay(int index) {
        return delays[index];
    }

    @Override
    public int getTrueDelay(int index) {
        return trueDelays==null ? -1 : trueDelays[index];
    }

    @Override
    public int binarySearch(long begin) {
        return Arrays.binarySearch(dates, 0, size, begin);
    }
}
