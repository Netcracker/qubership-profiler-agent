package org.qubership.profiler.test.util.cache;

import static org.testng.Assert.assertEquals;

import org.qubership.profiler.util.cache.TLimitedLongIntHashMap;

import org.testng.annotations.Test;

@Test(dataProviderClass = TestHashMapDataProvider.class)
public class TestTLimitedLongIntHashMap {
    @Test(dataProvider = "big-tests", groups = "big")
    public void doesNotCrashWhenManyMissesHappen(int size) {
        TLimitedLongIntHashMap map = new TLimitedLongIntHashMap(size);
        for (int i = 0; i < size * 10; i++)
            map.put(i, i);
    }

    @Test(dataProvider = "big-tests", groups = "big")
    public void cachesSingleValue(int size) {
        TLimitedLongIntHashMap map = new TLimitedLongIntHashMap(size);
        long key = 0x1234567812345678l;
        long j = key;
        int value = 0x1234567;
        map.put(key, value);
        for (int i = 0; i < size * 5; i++) {
            final int cached = map.get(key);
            assertEquals(cached, value, "Cached value does not match expectation at step " + i + ", mapsize " + map.size() + ", limit of " + size);
            j = j * 37 + 13;
            map.put(j, i);
            if (j == key)
                value = i;
        }
    }

    @Test(dataProvider = "big-tests", groups = "big", dependsOnMethods = {"doesNotCrashWhenManyMissesHappen", "cachesSingleValue"})
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
            final int cached = map.get(j);
            j = j * 37 + 13;
            assertEquals(cached, i, "Cached value does not match expectation at step " + i + ", mapsize " + map.size() + ", limit of " + size);
        }
    }
}
