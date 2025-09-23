package com.netcracker.profiler.io;

import com.netcracker.profiler.configuration.PropertyFacade;
import com.netcracker.profiler.dom.GanttInfo;

import java.util.*;

public class Hotspot {
    private final static int MAX_PARAMS = PropertyFacade.getProperty(Hotspot.class.getName() + ".MAX_PARAMS", 256);

    public int id;
    public ArrayList<Hotspot> children;
    public Map<HotspotTag, HotspotTag> tags;
    public PriorityQueue<HotspotTag> mostImportantTags;
    public int reactorCallId;
    public Set<Long> lastAssemblyId;
    public long lastParentAssemblyId;
    public byte isReactorEndPoint;
    public byte isReactorFrame;
    public int reactorDuration;
    public long reactorStartTime;
    public long reactorLeastTime;
    public int emit;

    public int blockingOperator;
    public int prevOperation;
    public int currentOperation;

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

    public void tag(long time, int tagId, int valueId, Object value, long assemblyId) {
        if (tags == null)
            tags = new HashMap<HotspotTag, HotspotTag>();
        final HotspotTag hs = new HotspotTag(tagId, value, assemblyId);
        tags.put(hs, hs);
    }

    public Hotspot getOrCreateChild(int tagId, long lastParentAssemblyId) {
        ArrayList<Hotspot> children = this.children;
        if (children == null)
            children = this.children = new ArrayList<Hotspot>();
        else
            for (final Hotspot child : children)
                if (child.id == tagId)
                    return child;

        Hotspot hs = new Hotspot(tagId);
        children.add(hs);
        return hs;
    }

    public Hotspot getOrCreateChild(int tagId) {
        ArrayList<Hotspot> children = this.children;
        if (children == null)
            children = this.children = new ArrayList<Hotspot>();
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
            tags = this.tags = new HashMap<HotspotTag, HotspotTag>();
        }

        for (HotspotTag hsTag : hsTags.values()) {
            final HotspotTag tag = tags.get(hsTag);
            if (tag == null) {
                final HotspotTag newTag = hsTag.dup();
                newTag.totalTime = hsTime + hs.reactorDuration;
                newTag.assemblyId = hsTag.assemblyId;
                addTag(tags, newTag);
                continue;
            }
            tag.totalTime += hsTime;
            tag.count += hsTag.count;
        }
    }

    public void addTag(Map<HotspotTag, HotspotTag> tags, HotspotTag newTag) {
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
        HotspotTag other = new HotspotTag(evicted.id);
        HotspotTag existingOther = tags.get(other);
        if (existingOther == null) {
            tags.put(other, other);
        } else {
            existingOther.totalTime += other.totalTime;
            existingOther.count += other.count;
        }
    }

    public void mergeWithChildren(Hotspot hs, List<GanttInfo> infos) {
        childTime += hs.childTime;
        totalTime += hs.totalTime;
        childCount += hs.childCount;
        count += hs.count;
        suspensionTime += hs.suspensionTime;
        childSuspensionTime += hs.childSuspensionTime;

        if (hs.lastAssemblyId != null) {
            if (lastAssemblyId == null) {
                lastAssemblyId = new HashSet<>();
            }
            lastAssemblyId.addAll(hs.lastAssemblyId);
        }

        if (reactorStartTime != 0 && hs.reactorStartTime != 0) {
            reactorStartTime = Math.min(hs.reactorStartTime, reactorStartTime);
        } else if (hs.reactorStartTime != 0) {
            reactorStartTime = hs.reactorStartTime;
        }

        if (hs.reactorStartTime != 0) {
            reactorLeastTime = Math.max(reactorLeastTime, hs.reactorStartTime + hs.reactorDuration);
            reactorDuration = (int) (reactorLeastTime - reactorStartTime);
        }

        if (startTime > hs.startTime) startTime = hs.startTime;
        if (endTime < hs.endTime) endTime = hs.endTime;

        if (hs.children != null) {
            if (children == null)
                children = hs.children.isEmpty() ? null : hs.children;
            else {
                final int childrenSize = children.size();
                for (Hotspot srcChild : hs.children) {
                    if (hs.fullRowId != null && infos != null) {
                        infos.add(
                                new GanttInfo(
                                        srcChild.id,
                                        srcChild.emit,
                                        srcChild.isReactorFrame == 0 ? srcChild.startTime : srcChild.reactorStartTime,
                                        srcChild.isReactorFrame == 0 ? srcChild.totalTime : srcChild.reactorDuration,
                                        hs.fullRowId,
                                        hs.folderId
                                        )
                        );
                    }
                    for (int i = 0; i < childrenSize; i++) {
                        Hotspot child = children.get(i);
                        if (child.id == srcChild.id && child.isReactorFrame == 0) {
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

    public void mergeWithChildren(Hotspot hs) {
        mergeWithChildren(hs, null);
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

        Map<HotspotTag, HotspotTag> newTags = new HashMap<HotspotTag, HotspotTag>((int) (tags.size() / 0.75f), 0.75f);

        for (HotspotTag tag : tags.values()) {
            newId = id2id.get(tag.id);
            if (newId != null)
                tag.id = newId;
            addTag(newTags, tag);
        }

        this.tags = newTags;
    }

}
