package com.netcracker.profiler.test.util.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.netcracker.profiler.util.cache.TLimitedLongLongHashMap;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class TestTLimitedLongLongHashMap {
    @Tag("big")
    @ParameterizedTest
    @MethodSource("com.netcracker.profiler.test.util.cache.TestHashMapDataProvider#createInstances")
    public void doesNotCrashWhenManyMissesHappen(int size) {
        TLimitedLongLongHashMap map = new TLimitedLongLongHashMap(size);
        for (int i = 0; i < size * 10; i++)
            map.put(i, i);
    }

    @Tag("big")
    @ParameterizedTest
    @MethodSource("com.netcracker.profiler.test.util.cache.TestHashMapDataProvider#createInstances")
    public void cachesSingleValue(int size) {
        TLimitedLongLongHashMap map = new TLimitedLongLongHashMap(size);
        long key = 0x1234567812345678l;
        long j = key;
        long value = 0x12345677654321l;
        map.put(key, value);
        for (int i = 0; i < size * 5; i++) {
            int stepIndex = i;
            long cached = map.get(key);
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
        TLimitedLongLongHashMap map = new TLimitedLongLongHashMap(size);
        long j = 0x1234567812345678l;
        for (int i = 0; i < size / 2 - 1; i++) {
            map.put(j, i * 1000000000l);
            map.get(j);
            j = j * 37 + 13;
        }

        for (int i = 0; i < size / 2 - 1; i++) {
            map.put(j, (i + 1) * 100);
            j = j * 37 + 13;
        }

        j = 0x1234567812345678l;
        for (int i = 0; i < size / 2 - 1; i++) {
            int stepIndex = i;
            long cached = map.get(j);
            assertEquals(i * 1000000000l, cached, () -> "Cached value does not match expectation at step " + stepIndex + ", mapsize " + map.size() + ", limit of " + size);
            j = j * 37 + 13;
        }
    }
}
