package com.netcracker.profiler.io;

import java.util.*;
import java.util.function.Consumer;

public class HotspotTag {
    public static java.util.Comparator<HotspotTag> COMPARATOR = new Comparator();

    public static final Set<Object> OTHER = Collections.singleton("::other");

    public int id;
    public int count = 1;
    public long totalTime;
    public Set<Object> values;
    // Hash codes for order-insensitive comparison
    private int sumHash;
    private int xorHash;

    public static class Comparator implements java.util.Comparator<HotspotTag> {
        public int compare(HotspotTag a, HotspotTag b) {
            long u = a.totalTime;
            long v = b.totalTime;
            return Long.compare(u, v);
        }
    }

    public static class Builder {
        private final Map<Integer, HotspotTag> values = new LinkedHashMap<>();

        public void clear() {
            values.clear();
        }

        public void addValue(int id, Object value) {
            values.computeIfAbsent(id, HotspotTag::new).addValue(value);
        }

        public void forEachTag(Consumer<HotspotTag> tagConsumer) {
            values.values().forEach(tagConsumer);
        }
    }

    public HotspotTag(int id) {
        this(id, new LinkedHashSet<>());
    }

    private HotspotTag(int id, Set<Object> values) {
        this.id = id;
        this.values = values;
    }

    public static HotspotTag ofOther(int id) {
        return new HotspotTag(id, OTHER);
    }

    public void addValue(Object value) {
        values.add(value);
        int hashCode = value.hashCode();
        sumHash += hashCode;
        xorHash ^= hashCode;
    }

    public HotspotTag dup() {
        HotspotTag tag = new HotspotTag(id, values);
        tag.count = count;
        tag.totalTime = totalTime;
        tag.sumHash = sumHash;
        tag.xorHash = xorHash;
        return tag;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HotspotTag that = (HotspotTag) o;

        if (id != that.id) return false;
        if (sumHash != that.sumHash) return false;
        if (xorHash != that.xorHash) return false;
        return values.equals(that.values);
    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + sumHash;
        result = 31 * result + xorHash;
        return result;
    }

    @Override
    public String toString() {
        return "HotspotTag{" +
                "id=" + id +
                ", count=" + count +
                ", totalTime=" + totalTime +
                ", values=" + values +
                '}';
    }
}
