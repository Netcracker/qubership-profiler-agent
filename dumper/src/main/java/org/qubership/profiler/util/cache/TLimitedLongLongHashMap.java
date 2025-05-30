package org.qubership.profiler.util.cache;

import gnu.trove.map.hash.TLongLongHashMap;

public class TLimitedLongLongHashMap extends TLongLongHashMap {
    final int maxSize;
    final static long IS_FREQUENT = 1L << 63;
    int clock = 0;

    public TLimitedLongLongHashMap() {
        this(1000);
    }

    public TLimitedLongLongHashMap(int maxSize) {
        this.maxSize = maxSize;
        setAutoCompactionFactor(0.0f);
    }

    @Override
    public long get(long key) {
        int index = index(key);
        if (index < 0) return -1;
        final long value = _values[index];
        _values[index] = value | IS_FREQUENT;
        return value & ~IS_FREQUENT;
    }

    @Override
    public long put(long key, long value) {
        if (size() >= maxSize)
            evictStale(Math.max((int) (maxSize * 0.15f), 1));
        return super.put(key, value);
    }

    private void evictStale(int killsLeft) {
        final long[] values = _values;
        final byte[] states = _states;
        final long[] set = _set;
        final int length = set.length;
        int clock;
        for (clock = this.clock; killsLeft > 0;) {
            if (states[clock] == FULL) {
                final long val = values[clock];
                if ((val & IS_FREQUENT) != 0)
                    values[clock] = val & ~IS_FREQUENT;
                else {
                    removeAt(clock);
                    killsLeft--;
                }
            }
            clock++;
            if (clock == length) clock = 0;
        }
        this.clock = clock;
    }
}
