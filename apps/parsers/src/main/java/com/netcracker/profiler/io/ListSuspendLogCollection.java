package com.netcracker.profiler.io;

import java.util.*;

public class ListSuspendLogCollection implements ISuspendLogCollection {

    private List<SuspendLogPair<Long, Integer>> datesWithDelays = new ArrayList<>();

    public List<SuspendLogPair<Long, Integer>> getDatesWithDelays() {
        return datesWithDelays;
    }

    @Override
    public int size() {
        return datesWithDelays.size();
    }

    @Override
    public long getDate(int index) {
        return datesWithDelays.get(index).getDateOfSuspend();
    }

    @Override
    public int getDelay(int index) {
        return datesWithDelays.get(index).getDelay();
    }

    @Override
    public int getTrueDelay(int index) {
        return -1;
    }

    @Override
    public int binarySearch(long begin) {
        return Collections.binarySearch(datesWithDelays, new SuspendLogPair<>(begin, null),
                new Comparator<SuspendLogPair<Long, ?>>() {
                    @Override
                    public int compare(SuspendLogPair<Long, ?> o1, SuspendLogPair<Long, ?> o2) {
                        return Long.compare(o1.getDateOfSuspend(), o2.getDateOfSuspend());
                    }
                });
    }
}
