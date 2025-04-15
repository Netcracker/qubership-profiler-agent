package org.qubership.profiler.test.util.cache;

import static org.testng.Assert.assertEquals;

import org.qubership.profiler.util.cache.TLimitedLongLongHashMap;

import org.testng.annotations.Test;

@Test(dataProviderClass = TestHashMapDataProvider.class)
public class TestTLimitedLongLongHashMap {
    @Test(dataProvider = "big-tests", groups = "big")
    public void doesNotCrashWhenManyMissesHappen(int size) {
        TLimitedLongLongHashMap map = new TLimitedLongLongHashMap(size);
        for (int i = 0; i < size * 10; i++)
            map.put(i, i);
    }

    @Test(dataProvider = "big-tests", groups = "big")
    public void cachesSingleValue(int size) {
        TLimitedLongLongHashMap map = new TLimitedLongLongHashMap(size);
        long key = 0x1234567812345678l;
        long j = key;
        long value = 0x12345677654321l;
        map.put(key, value);
        for (int i = 0; i < size * 5; i++) {
            final long cached = map.get(key);
            assertEquals(cached, value, "Cached value does not match expectation at step " + i + ", mapsize " + map.size() + ", limit of " + size);
            j = j * 37 + 13;
            map.put(j, i);
            if (j == key)
                value = i;
        }
    }

    @Test(dataProvider = "big-tests", groups = "big", dependsOnMethods = {"doesNotCrashWhenManyMissesHappen", "cachesSingleValue"})
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
            final long cached = map.get(j);
            assertEquals(cached, i * 1000000000l, "Cached value does not match expectation at step " + i + ", mapsize " + map.size() + ", limit of " + size);
            j = j * 37 + 13;
        }
    }
}
