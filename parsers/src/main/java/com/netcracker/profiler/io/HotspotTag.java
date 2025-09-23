package com.netcracker.profiler.io;

import java.util.ArrayList;
import java.util.List;

public class HotspotTag {
    public static java.util.Comparator<HotspotTag> COMPARATOR = new Comparator();

    public static final String OTHER = "::other";

    public int id;
    public int count = 1;
    public long assemblyId;
    public long totalTime;
    public Object value;

    public long reactorStartDate;
    public byte isParallel;
    public List<Pair<Integer, Integer>> parallels = new ArrayList<>();

    public static class Comparator implements java.util.Comparator<HotspotTag> {
        public int compare(HotspotTag a, HotspotTag b) {
            long u = a.totalTime;
            long v = b.totalTime;
            return Long.compare(u, v);
        }
    }

    public HotspotTag(int id) {
        this(id, OTHER);
    }

    public HotspotTag(int id, Object value) {
        this.id = id;
        this.value = value;
    }

    public HotspotTag(int id, Object value, long assemblyId) {
        this.id = id;
        this.value = value;
        this.assemblyId = assemblyId;
    }

    public HotspotTag dup() {
        final HotspotTag tag = new HotspotTag(id, value);
        tag.count = count;
        tag.totalTime = totalTime;
        return tag;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HotspotTag that = (HotspotTag) o;

        if (id != that.id) return false;
        return value.equals(that.value);
    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + value.hashCode();
        return result;
    }
}
