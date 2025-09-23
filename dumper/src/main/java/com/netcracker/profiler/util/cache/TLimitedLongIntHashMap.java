package com.netcracker.profiler.util.cache;

import gnu.trove.map.hash.TLongIntHashMap;

public class TLimitedLongIntHashMap extends TLongIntHashMap {
    final int maxSize;
    final int IS_FREQUENT = 0x80000000;
    int clock = 0;

    public TLimitedLongIntHashMap() {
        this(1000);
    }

    public TLimitedLongIntHashMap(int maxSize) {
        this.maxSize = maxSize;
        setAutoCompactionFactor(0.0f);
    }

    @Override
    public int get(long key) {
        int index = index(key);
        if (index < 0) return -1;
        final int value = _values[index];
        _values[index] = value | IS_FREQUENT;
        return value & ~IS_FREQUENT;
    }

    @Override
    public int put(long key, int value) {
        if (size() >= maxSize)
            evictStale(Math.max((int) (maxSize * 0.15f), 1));
        return super.put(key, value);
    }

    private void evictStale(int killsLeft) {
        final int[] values = _values;
        final byte[] states = _states;
        final long[] set = _set;
        final int length = set.length;
        int clock;
        for (clock = this.clock; killsLeft > 0;) {
            if (states[clock] == FULL) {
                final int val = values[clock];
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
