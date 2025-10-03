package com.netcracker.profiler.io;

import com.netcracker.profiler.configuration.PropertyFacade;

import java.util.*;

public class Hotspot {
    private final static int MAX_PARAMS = PropertyFacade.getProperty(Hotspot.class.getName() + ".MAX_PARAMS", 256);

    public int id;
    public ArrayList<Hotspot> children;
    public Map<HotspotTag, HotspotTag> tags;
    public PriorityQueue<HotspotTag> mostImportantTags;

    public String fullRowId;
    public int folderId;
    public long childTime;
    public long totalTime;
    public int childCount;
    public int count;
    public int suspensionTime;
    public int childSuspensionTime;
    public long startTime = Long.MAX_VALUE, endTime = Long.MIN_VALUE;

    public Hotspot(int id) {
        this.id = id;
    }

    public void addTag(HotspotTag tag) {
        if (tags == null) {
            tags = new LinkedHashMap<>();
        }
        addTag(tags, tag);
    }

    public Hotspot getOrCreateChild(int tagId) {
        ArrayList<Hotspot> children = this.children;
        if (children == null)
            children = this.children = new ArrayList<>();
        else
            for (final Hotspot child : children)
                if (child.id == tagId)
                    return child;

        Hotspot hs = new Hotspot(tagId);
        children.add(hs);
        return hs;
    }

    public void merge(Hotspot hs) {
        final long hsTime = hs.totalTime;
        totalTime += hsTime;
        suspensionTime += hs.suspensionTime;
        childTime += hs.childTime;
        count += hs.count;
        if (startTime > hs.startTime) startTime = hs.startTime;
        if (endTime < hs.endTime) endTime = hs.endTime;
        final Map<HotspotTag, HotspotTag> hsTags = hs.tags;
        if (hsTags == null || hsTags.isEmpty()) {
            return;
        }

        Map<HotspotTag, HotspotTag> tags = this.tags;
        if (tags == null) {
            tags = this.tags = new LinkedHashMap<>();
        }

        for (HotspotTag hsTag : hsTags.values()) {
            final HotspotTag tag = tags.get(hsTag);
            if (tag == null) {
                final HotspotTag newTag = hsTag.dup();
                newTag.totalTime = hsTime;
                addTag(tags, newTag);
                continue;
            }
            tag.totalTime += hsTime;
            tag.count += hsTag.count;
        }
    }

    private void addTag(Map<HotspotTag, HotspotTag> tags, HotspotTag newTag) {
        if (tags.size() < MAX_PARAMS) {
            tags.put(newTag, newTag);
            return;
        }

        if (mostImportantTags == null) {
            mostImportantTags = new PriorityQueue<HotspotTag>(MAX_PARAMS, HotspotTag.COMPARATOR);
            for (HotspotTag tag : tags.keySet()) {
                mostImportantTags.add(tag);
            }
        }
        HotspotTag first = mostImportantTags.peek();
        HotspotTag evicted;
        if (newTag.totalTime <= first.totalTime) {
            // If newTag is smaller than the smallest in the queue, just discard newTag
            evicted = newTag;
        } else {
            // first should be evicted
            evicted = first;
            HotspotTag smallestTag = mostImportantTags.poll();
            tags.remove(smallestTag);

            mostImportantTags.add(newTag);
            tags.put(newTag, newTag);
        }
        HotspotTag other = HotspotTag.ofOther(evicted.id);
        other.totalTime = evicted.totalTime;
        other.count = evicted.count;
        HotspotTag existingOther = tags.get(other);
        if (existingOther == null) {
            tags.put(other, other);
        } else {
            existingOther.totalTime += other.totalTime;
            existingOther.count += other.count;
        }
    }

    public void mergeWithChildren(Hotspot hs) {
        childTime += hs.childTime;
        totalTime += hs.totalTime;
        childCount += hs.childCount;
        count += hs.count;
        suspensionTime += hs.suspensionTime;
        childSuspensionTime += hs.childSuspensionTime;

        if (startTime > hs.startTime) startTime = hs.startTime;
        if (endTime < hs.endTime) endTime = hs.endTime;

        if (hs.children != null) {
            if (children == null)
                children = hs.children.isEmpty() ? null : hs.children;
            else {
                final int childrenSize = children.size();
                for (Hotspot srcChild : hs.children) {
                    for (int i = 0; i < childrenSize; i++) {
                        Hotspot child = children.get(i);
                        if (child.id == srcChild.id) {
                            child.mergeWithChildren(srcChild);
                            srcChild = null;
                            break;
                        }
                    }
                    if (srcChild == null) continue;
                    children.add(srcChild);
                }
            }
        }

        final Map<HotspotTag, HotspotTag> hsTags = hs.tags;
        if (hsTags == null || hsTags.isEmpty()) {
            return;
        }

        Map<HotspotTag, HotspotTag> tags = this.tags;

        if (tags == null) {
            this.tags = hsTags;
            return;
        }

        for (HotspotTag hsTag : hsTags.values()) {
            final HotspotTag tag = tags.get(hsTag);
            if (tag == null) {
                addTag(tags, hsTag);
                continue;
            }
            tag.totalTime += hsTag.totalTime;
            tag.count += hsTag.count;
        }
    }

    public void calculateTotalExecutions() {
        calculateTotalExecutions(new Hotspot(0));
    }

    protected void calculateTotalExecutions(Hotspot prev) {
        if (children != null)
            for (Hotspot child : children)
                child.calculateTotalExecutions(this);

        prev.childTime += totalTime;
        prev.childCount += count + childCount;
        prev.childSuspensionTime += suspensionTime + childSuspensionTime;

        childTime -= childSuspensionTime;
        totalTime -= childSuspensionTime + suspensionTime;
    }

    @Deprecated
    public Map<Integer, Hotspot> flatProfile() {
        calculateTotalExecutions();
        // this function should use parameters' signatures
        // currently, only javascript has proper implementation
        return Collections.emptyMap();
    }

    public void remap(Map<Integer, Integer> id2id) {
        if (id2id.isEmpty()) return;
        Integer newId = id2id.get(id);
        if (newId != null)
            id = newId;

        if (children != null)
            for (Hotspot child : children)
                child.remap(id2id);

        final Map<HotspotTag, HotspotTag> tags = this.tags;
        if (tags == null || tags.isEmpty()) return;

        Map<HotspotTag, HotspotTag> newTags = new LinkedHashMap<>((int) (tags.size() / 0.75f), 0.75f);

        for (HotspotTag tag : tags.values()) {
            newId = id2id.get(tag.id);
            if (newId != null)
                tag.id = newId;
            addTag(newTags, tag);
        }

        this.tags = newTags;
    }

}
