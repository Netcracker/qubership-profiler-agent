package org.qubership.profiler.util.cache;

import gnu.trove.map.hash.TIntObjectHashMap;

import java.util.BitSet;

public class TLimitedIntObjectHashMap<V> extends TIntObjectHashMap<V> {
    final int maxSize;
    final int IS_FREQUENT = 0x80000000;
    int clock = 0;
    BitSet frequency;

    public TLimitedIntObjectHashMap() {
        this(1000);
    }

    public TLimitedIntObjectHashMap(int maxSize) {
        this.maxSize = maxSize;
        frequency = new BitSet(maxSize);
        setAutoCompactionFactor(0.0f);
    }

    @Override
    public V get(int key) {
        int index = index(key);
        if (index < 0) return null;
        frequency.set(index);
        return _values[index];
    }

    @Override
    public V put(int key, V value) {
        if (size() >= maxSize)
            evictStale(Math.max((int) (maxSize * 0.15f), 1));
        return super.put(key, value);
    }

    private void evictStale(int killsLeft) {
        BitSet frequency = this.frequency;
        final byte[] states = _states;
        final int[] set = _set;
        final int length = set.length;
        int clock;
        for (clock = this.clock; killsLeft > 0;) {
            if (states[clock] == FULL) {
                if (!frequency.get(clock)) {
                    removeAt(clock);
                    killsLeft--;
                }
                frequency.clear(clock);
            }
            clock++;
            if (clock == length) clock = 0;
        }
        this.clock = clock;
    }
}
