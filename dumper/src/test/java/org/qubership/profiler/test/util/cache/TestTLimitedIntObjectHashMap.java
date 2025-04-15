package org.qubership.profiler.test.util.cache;

import static org.testng.Assert.assertEquals;

import org.qubership.profiler.util.cache.TLimitedIntObjectHashMap;

import org.testng.annotations.Test;

@Test(dataProviderClass = TestHashMapDataProvider.class)
public class TestTLimitedIntObjectHashMap {
    @Test(dataProvider = "big-tests", groups = "big")
    public void doesNotCrashWhenManyMissesHappen(int size) {
        TLimitedIntObjectHashMap<Long> map = new TLimitedIntObjectHashMap<Long>(size);
        for (int i = 0; i < size * 10; i++)
            map.put(i, (long) i * 10000);
    }

    @Test(dataProvider = "big-tests", groups = "big")
    public void cachesSingleValue(int size) {
        TLimitedIntObjectHashMap<Long> map = new TLimitedIntObjectHashMap<Long>(size);
        int key = 0x12345678;
        int j = key;
        Long value = 0x12345677654321l;
        map.put(key, value);
        for (int i = 0; i < size * 5; i++) {
            final Long cached = map.get(key);
            assertEquals(cached, value, "Cached value does not match expectation at step " + i + ", mapsize " + map.size() + ", limit of " + size);
            j = j * 37 + 13;
            map.put(j, (long) i);
            if (j == key)
                value = (long) i;
        }
    }

    @Test(dataProvider = "big-tests", groups = "big", dependsOnMethods = {"doesNotCrashWhenManyMissesHappen", "cachesSingleValue"})
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
            final long cached = map.get(j);
            assertEquals(cached, i * 1000000000l, "Cached value does not match expectation at step " + i + ", mapsize " + map.size() + ", limit of " + size);
            j = j * 37 + 13;
        }
    }
}
