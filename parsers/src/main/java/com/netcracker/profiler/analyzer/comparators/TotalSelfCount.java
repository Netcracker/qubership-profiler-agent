package com.netcracker.profiler.analyzer.comparators;

import com.netcracker.profiler.io.Hotspot;

import java.util.Comparator;

public class TotalSelfCount implements Comparator<Hotspot> {
    public static final Comparator<Hotspot> INSTANCE = new TotalSelfCount();

    public int compare(Hotspot a, Hotspot b) {
        long x = b.totalTime;
        long y = a.totalTime;
        if (x > y) return 1;
        if (x < y) return -1;

        x -= b.childTime;
        y -= a.childTime;
        if (x > y) return 1;
        if (x < y) return -1;

        x = b.suspensionTime + b.childSuspensionTime;
        y = a.suspensionTime + a.childSuspensionTime;
        if (x > y) return 1;
        if (x < y) return -1;

        x = b.childSuspensionTime;
        y = a.childSuspensionTime;
        if (x > y) return 1;
        if (x < y) return -1;

        x = b.count + b.childCount;
        y = a.count + a.childCount;
        if (x > y) return 1;
        if (x < y) return -1;

        x = b.count;
        y = a.count;
        if (x > y) return 1;
        if (x < y) return -1;

        return 0;
    }
}
