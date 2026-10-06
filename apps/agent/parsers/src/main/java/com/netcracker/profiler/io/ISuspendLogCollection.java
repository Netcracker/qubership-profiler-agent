package com.netcracker.profiler.io;

public interface ISuspendLogCollection {

    int size();
    long getDate(int index);
    int getDelay(int index);
    int getTrueDelay(int index);
    int binarySearch(long begin);

}
