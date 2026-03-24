package com.netcracker.common.models;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class SuspendRange {
    public static final Comparator<Pair> COMPARATOR = Comparator.comparingLong(Pair::t);

    record Pair(long t, int delay) {
    }

    private List<Pair> list = new ArrayList<>();

    public int size() {
        return list.size();
    }

    public void add(long t, int delay) {
        list.add(new Pair(t, delay));
    }

    public void addAll(SuspendRange found) {
        list.addAll(found.list);
        Collections.sort(list, COMPARATOR); // for binary
    }

    // Returns the net suspension time in the given time range [begin, end)
    public int getSuspendDuration(long begin, long end) {
        var cursor = cursor();
        cursor.skipTo(begin);
        return cursor.moveTo(end);
    }

    public int binarySearch(long begin) {
        return Collections.binarySearch(list, new Pair(begin, -1), COMPARATOR);
    }

    public Cursor cursor() {
        return new Cursor();
    }

    public class Cursor {
        public int idx;
        protected long now;
        protected long a;

        // Moves cursor to a new time position.
        public void skipTo(long begin) {
            int idx = binarySearch(begin);

            if (idx < 0) idx = -idx - 1;
            this.idx = idx;
            now = begin;
            if (idx == list.size()) return;
            long zT = list.get(idx).t;
            this.a = zT - list.get(idx).delay;
        }

        // Calculate net suspension time in the timerange [begin, end) and advances the cursor.
        public int moveTo(long end) {
            if (idx == list.size()) {
                return 0;
            }

            long a = this.a;

            if (a >= end) {
                return 0;
            }

            long zT = list.get(idx).t;

            float suspend = (int) Math.min(list.get(idx).delay, zT - now);
            if (zT >= end) {
                suspend -= zT - end;
                now = end;
                return (int) (suspend);
            }

            for (idx++; idx < list.size(); idx++) {
                zT = list.get(idx).t;
                int delay = list.get(idx).delay;
                if (zT < end) {
                    suspend += delay;
                    continue;
                }
                a = zT - delay;
                if (a < end)
                    suspend += (end - a);
                break;
            }
            now = end;
            this.a = a;
            return (int) suspend;
        }
    }
}
