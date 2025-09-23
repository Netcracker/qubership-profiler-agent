package com.netcracker.profiler.test.util.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.netcracker.profiler.util.cache.TLimitedIntObjectHashMap;

import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class TestTLimitedIntObjectHashMap {
    @Tag("big")
    @Order(1)
    @ParameterizedTest
    @MethodSource("com.netcracker.profiler.test.util.cache.TestHashMapDataProvider#createInstances")
    public void doesNotCrashWhenManyMissesHappen(int size) {
        TLimitedIntObjectHashMap<Long> map = new TLimitedIntObjectHashMap<Long>(size);
        for (int i = 0; i < size * 10; i++)
            map.put(i, (long) i * 10000);
    }

    @Tag("big")
    @Order(2)
    @ParameterizedTest
    @MethodSource("com.netcracker.profiler.test.util.cache.TestHashMapDataProvider#createInstances")
    public void cachesSingleValue(int size) {
        TLimitedIntObjectHashMap<Long> map = new TLimitedIntObjectHashMap<Long>(size);
        int key = 0x12345678;
        int j = key;
        Long value = 0x12345677654321l;
        map.put(key, value);
        for (int i = 0; i < size * 5; i++) {
            int stepIndex = i;
            Long cached = map.get(key);
            assertEquals(cached, value, () -> "Cached value does not match expectation at step " + stepIndex + ", mapsize " + map.size() + ", limit of " + size);
            j = j * 37 + 13;
            map.put(j, (long) i);
            if (j == key)
                value = (long) i;
        }
    }

    @Tag("big")
    @Order(3)
    @ParameterizedTest
    @MethodSource("com.netcracker.profiler.test.util.cache.TestHashMapDataProvider#createInstances")
    public void mapShouldKeepCachedValues(int size) {
        TLimitedIntObjectHashMap<Long> map = new TLimitedIntObjectHashMap<Long>(size);
        int j = 0x12345678;
        for (int i = 0; i < size / 2 - 1; i++) {
            map.put(j, i * 1000000000l);
            map.get(j);
            j = j * 37 + 13;
        }

        for (int i = 0; i < size / 2 - 1; i++) {
            map.put(j, (long) (i + 1) * 100);
            j = j * 37 + 13;
        }

        j = 0x12345678;
        for (int i = 0; i < size / 2 - 1; i++) {
            int stepIndex = i;
            long cached = map.get(j);
            assertEquals(i * 1000000000l, cached, () -> "Cached value does not match expectation at step " + stepIndex + ", mapsize " + map.size() + ", limit of " + size);
            j = j * 37 + 13;
        }
    }
}
