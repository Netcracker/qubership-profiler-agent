package com.netcracker.profiler.util.cache;

import java.util.Map;

public class ClockProCache<K, V> {
    Entry[] entries;
    int size;
    int coldCached;

    Map.Entry handHot, handTest, handCold;


    public ClockProCache(int cacheSize) {
        int capacity = (int) (cacheSize / 0.75);
        size = Integer.highestOneBit(capacity - 1) << 1;

        entries = new Entry[size];

        Entry<K, V> clock = new Entry<K, V>(0, null, null, null);
        clock.after = clock.before = clock;
    }

    public synchronized Entry<K, V> get(Object key) {
        Entry[] tab = entries;
        final int hash = key.hashCode();
        int idx = hash & (tab.length - 1);
        Entry<K, V> e;
        for (e = tab[idx]; e != null; e = e.next) {
            final K k;
            if (e.hash == hash && ((k = e.key) == key || key.equals(k)))
                break;
        }

        /* Just as in CLOCK, there are no operationsin CLOCK-Pro for page hits,
           only the reference bist of the accessed pages are set.
         */
        if (e != null) {

            /* Cache hit */
            e.status |= Entry.ACCESS_BIT | Entry.HOT_BIT; /* Cache hit */
            return e;
        }

        e = new Entry<K, V>(hash, (K) key, null, tab[idx]);
        tab[idx] = e;


        return e; /* TODO: call post loading callback? */
    }

    protected void onEvict(Entry<K, V> entry) {
    }

    private int indexFor(int hashCode) {
        return hashCode & (size - 1);
    }

    public static class Entry<K, V> {
        K key;
        V value;
        int hash;
        int status;
        static final int ACCESS_BIT = 1;
        static final int HOT_BIT = 2;

        Entry<K, V> before, after;
        Entry<K, V> next;

        public Entry(int hash, K key, V value, Entry<K, V> next) {
            this.hash = hash;
            this.key = key;
            this.value = value;
            this.next = next;
        }

        public K key() {
            return key;
        }

        public V value() {
            return value;
        }

        public V value(V newValue) {
            V old = value;
            value = newValue;
            return old;
        }
    }
}
