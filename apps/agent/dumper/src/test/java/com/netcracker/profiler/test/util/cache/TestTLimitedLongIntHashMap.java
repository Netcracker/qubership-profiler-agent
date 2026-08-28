package com.netcracker.profiler.test.util.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.netcracker.profiler.util.cache.TLimitedLongIntHashMap;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class TestTLimitedLongIntHashMap {
    @Tag("big")
    @ParameterizedTest
    @MethodSource("com.netcracker.profiler.test.util.cache.TestHashMapDataProvider#createInstances")
    public void doesNotCrashWhenManyMissesHappen(int size) {
        TLimitedLongIntHashMap map = new TLimitedLongIntHashMap(size);
        for (int i = 0; i < size * 10; i++)
            map.put(i, i);
    }

    @Tag("big")
    @ParameterizedTest
    @MethodSource("com.netcracker.profiler.test.util.cache.TestHashMapDataProvider#createInstances")
    public void cachesSingleValue(int size) {
        TLimitedLongIntHashMap map = new TLimitedLongIntHashMap(size);
        long key = 0x1234567812345678l;
        long j = key;
        int value = 0x1234567;
        map.put(key, value);
        for (int i = 0; i < size * 5; i++) {
            int stepIndex = i;
            int cached = map.get(key);
            assertEquals(value, cached, () -> "Cached value does not match expectation at step " + stepIndex + ", mapsize " + map.size() + ", limit of " + size);
            j = j * 37 + 13;
            map.put(j, i);
            if (j == key)
                value = i;
        }
    }

    @Tag("big")
    @ParameterizedTest
    @MethodSource("com.netcracker.profiler.test.util.cache.TestHashMapDataProvider#createInstances")
    public void mapShouldKeepCachedValues(int size) {
        TLimitedLongIntHashMap map = new TLimitedLongIntHashMap(size);
        long j = 0x1234567812345678l;
        for (int i = 0; i < size / 2 - 1; i++) {
            map.put(j, i);
            map.get(j);
            j = j * 37 + 13;
        }

        for (int i = 0; i < size / 2 - 1; i++) {
            map.put(j, (i + 1) * 100000);
            j = j * 37 + 13;
        }

        j = 0x1234567812345678l;
        for (int i = 0; i < size / 2 - 1; i++) {
            int stepIndex = i;
            int cached = map.get(j);
            j = j * 37 + 13;
            assertEquals(i, cached, () -> "Cached value does not match expectation at step " + stepIndex + ", mapsize " + map.size() + ", limit of " + size);
        }
    }
}
