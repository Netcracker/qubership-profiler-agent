package com.netcracker.cdt.ui.services.tree.data;

import com.netcracker.common.models.Pair;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class HotspotTag {
    public static final String OTHER = "::other";
    public static Comparator<HotspotTag> COMPARATOR = Comparator.comparingLong(a -> a.totalTime);

    public int id;
    public int count = 1;
    public long assemblyId;
    public long totalTime;
    public Object value;

    public long reactorStartDate;
    public byte isParallel;
    public List<Pair<Integer, Integer>> parallels = new ArrayList<>();

    HotspotTag(int id) {
        this(id, OTHER);
    }

    HotspotTag(int id, Object value, long assemblyId) {
        this.id = id;
        this.value = value;
        this.assemblyId = assemblyId;
    }

    protected HotspotTag(int id, Object value) {
        this.id = id;
        this.value = value;
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
