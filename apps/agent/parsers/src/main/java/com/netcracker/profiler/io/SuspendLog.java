package com.netcracker.profiler.io;


import java.util.*;

public class SuspendLog {
    public static final SuspendLog EMPTY = new SuspendLog(new long[0], new int[0]);
    public SuspendLogCursor cursor;
    protected ISuspendLogCollection suspendLogCollection;
    protected String podName;
    protected long loadedTo;
    protected long loadedFrom;
    protected List<SuspendLogPair<Long, Integer>> datesWithDelays;

    public SuspendLog(long[] dates, int[] delays) {
        this(dates, delays, null);
    }

    public SuspendLog(long[] dates, int[] delays, int[] trueDelays) {
        this.suspendLogCollection = new ArraySuspendLogCollection(dates, delays, trueDelays);
        this.cursor = cursor();
    }

    public SuspendLog(long[] dates, int[] delays, int[] trueDelays, int size) {
        this.suspendLogCollection = new ArraySuspendLogCollection(dates, delays, trueDelays, size);
        this.cursor = cursor();
    }

    public SuspendLog(String podName) {
        this.podName = podName;
        this.cursor = cursor();
    }



    /**
     * Returns the net suspension time in the given timerange [begin, end)
     *
     * @param begin the start of timerange for suspension calculation, inclusive
     * @param end   the end of timerange for suspension calculation, exclusive
     * @return the net suspension time
     */
    public int getSuspendDuration(long begin, long end) {
        cursor.skipTo(begin);
        return cursor.moveTo(end);
    }

    public SuspendLogCursor cursor() {
        return new SuspendLogCursor();
    }

    public void setValue(long[] dates, int[] delays, int[] trueDelays) {
        this.suspendLogCollection = new ArraySuspendLogCollection(dates, delays, trueDelays);
    }

    private float getK(int index) {
        return suspendLogCollection.getTrueDelay(index) == -1 || index >= suspendLogCollection.size() ? 1.0f : ((float)suspendLogCollection.getTrueDelay(index)) / suspendLogCollection.getDelay(index);
    }

    public class SuspendLogCursor {
        public int idx;
        protected long now;
        protected long a;
        protected int endTimeForSearch = 30000; // default range is 30 seconds

        /**
         * Moves cursor to a new time position.
         * The following moveTo call would calculate
         *
         * @param begin new time position
         */
        public void skipTo(long begin) {
            int idx = suspendLogCollection.binarySearch(begin);

            if (idx < 0) idx = -idx - 1;
            this.idx = idx;
            now = begin;
            if (idx == suspendLogCollection.size()) return;
            long z = suspendLogCollection.getDate(idx);
            this.a = z - suspendLogCollection.getDelay(idx);
        }

        /**
         * Calculate suspension time in the timerange [begin, end) and advances the cursor.
         *
         * @param end the end of interval (exclusive) for suspension time calculation
         * @return net suspension time in interval [begin, end)
         */
        public int moveTo(long end) {
            if (idx == suspendLogCollection.size()) {
                return 0;
            }

            long a = this.a;

            if (a >= end) {
                return 0;
            }

            long z = suspendLogCollection.getDate(idx);

            float suspend = (int) Math.min(suspendLogCollection.getDelay(idx), z - now);
            if (z >= end) {
                suspend -= z - end;
                now = end;
                return (int) (suspend * getK(idx));
            }
            suspend *= getK(idx);

            for (idx++; idx < suspendLogCollection.size(); idx++) {
                z = suspendLogCollection.getDate(idx);
                int delay = suspendLogCollection.getDelay(idx);
                float k = getK(idx);
                if (z < end) {
                    suspend += delay * k;
                    continue;
                }
                a = z - delay;
                if (a < end)
                    suspend += (end - a) * k;
                break;
            }
            now = end;
            this.a = a;
            return (int) suspend;
        }
    }

    public int size() {
        return suspendLogCollection.size();
    }
}
