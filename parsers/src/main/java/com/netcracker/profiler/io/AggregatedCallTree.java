package com.netcracker.profiler.io;

import com.netcracker.profiler.configuration.ParameterInfoDto;

import java.util.*;

public class AggregatedCallTree {
    public final Hotspot root;
    public final Map<Integer, Hotspot> flatProfile;
    public List<String> tags;
    public final Map<String, ParameterInfoDto> paramInfo;
    public final Map<Integer, Map<Integer, String>> dedupParams;
    public final Map<Integer, Map<Integer, String>> bigParams;
    public final BitSet requredIds;

    private static final String UNMODIFIABLE_LIST_CLASS_NAME =
        Collections.unmodifiableList(Collections.emptyList()).getClass().getName();

    private Map<String, Object> dedupParamsMap;
    private Map<String, Integer> tagsMap;
    int nextDedupParamsOffset;

    public AggregatedCallTree(Hotspot root, Map<Integer, Hotspot> flatProfile, List<String> tags, Map<String, ParameterInfoDto> paramInfo, Map<Integer, Map<Integer, String>> dedupParams, Map<Integer, Map<Integer, String>> bigParams, BitSet requredIds) {
        this.root = root;
        this.flatProfile = flatProfile;
        this.tags = tags;
        this.paramInfo = paramInfo;
        this.dedupParams = dedupParams;
        this.bigParams = bigParams;
        this.requredIds = requredIds;
    }

    private void ensureMapsCreated() {
        if (dedupParamsMap != null) return;
        Map<String, Object> dedupParamsMap = new HashMap<String, Object>();
        for (Map.Entry<Integer, Map<Integer, String>> entry : dedupParams.entrySet()) {
            int fileId = entry.getKey();
            for (Map.Entry<Integer, String> value : entry.getValue().entrySet()) {
                dedupParamsMap.put(value.getValue(), new BigDedupParamKey(fileId, value.getKey()));
            }
        }

        for (Map.Entry<Integer, Map<Integer, String>> entry : bigParams.entrySet()) {
            int fileId = entry.getKey();
            for (Map.Entry<Integer, String> value : entry.getValue().entrySet()) {
                dedupParamsMap.put(value.getValue(), new BigParamKey(fileId, value.getKey()));
            }
        }

        this.dedupParamsMap = dedupParamsMap;
        dedupParams.put(-1, new HashMap<Integer, String>());

        if (UNMODIFIABLE_LIST_CLASS_NAME.equals(tags.getClass().getName()))
            tags = new ArrayList<String>(tags);

        HashMap<String, Integer> tagsMap = new HashMap<String, Integer>((int) (requredIds.cardinality() / 0.5));
        for (int i = -1; (i = requredIds.nextSetBit(i + 1)) >= 0;) {
            String tag = tags.get(i);
            tagsMap.put(tag, i);
        }
        this.tagsMap = tagsMap;
    }

    public void merge(AggregatedCallTree tree) {
        ensureMapsCreated();

        remap(tree);
        if (root.id != tree.root.id)
            throw new IllegalArgumentException("Unable to merge two trees with different root ids: " + root.id + " and " + tree.root.id);
        root.mergeWithChildren(tree.root);
    }

    private void remap(AggregatedCallTree tree) {
        final Map<String, Integer> tagsMap = this.tagsMap;
        final List<String> tags = this.tags;
        final BitSet requiredIds = requredIds;

        HashMap<Integer, Integer> id2id = new HashMap<Integer, Integer>();
        final BitSet treeIds = tree.requredIds;
        for (int i = -1; (i = treeIds.nextSetBit(i + 1)) >= 0;) {
            String tag = tree.tags.get(i);
            if (tag == null) continue;
            Integer newTagId = tagsMap.get(tag);
            if (newTagId == null) {
                final int tagsSize = tags.size();
                if (!requiredIds.get(i) && i < tagsSize) {
                    tags.set(i, tag);
                    requiredIds.set(i);
                    continue;
                }
                newTagId = tagsSize;
                tagsMap.put(tag, newTagId);
                requiredIds.set(newTagId);
                tags.add(tag);
            }
            if (newTagId == i) continue;
            id2id.put(i, newTagId);
        }

        final Map<String, Object> dedupParamsMap = this.dedupParamsMap;
        final Map<Integer, String> dedupParams = this.dedupParams.get(-1);
        HashMap big2big = new HashMap();

        for (Map.Entry<Integer, Map<Integer, String>> entry : tree.dedupParams.entrySet()) {
            int fileId = entry.getKey();
            for (Map.Entry<Integer, String> value : entry.getValue().entrySet()) {
                final String param = value.getValue();
                Object newDedupParamKey = dedupParamsMap.get(param);
                if (newDedupParamKey == null) {
                    BigDedupParamKey key = new BigDedupParamKey(-1, nextDedupParamsOffset++);
                    dedupParams.put(key.offset, param);
                    dedupParamsMap.put(param, key);
                    newDedupParamKey = key;
                }
                big2big.put(new BigDedupParamKey(fileId, value.getKey()), newDedupParamKey);
            }
        }

        for (Map.Entry<Integer, Map<Integer, String>> entry : tree.bigParams.entrySet()) {
            int fileId = entry.getKey();
            for (Map.Entry<Integer, String> value : entry.getValue().entrySet()) {
                final String param = value.getValue();
                Object newBigParamKey = dedupParamsMap.get(param);
                if (newBigParamKey == null) {
                    BigDedupParamKey key = new BigDedupParamKey(-1, nextDedupParamsOffset++);
                    dedupParams.put(key.offset, param);
                    dedupParamsMap.put(param, key);
                    newBigParamKey = key;
                }
                big2big.put(new BigParamKey(fileId, value.getKey()), newBigParamKey);
            }
        }

        final Map<String, ParameterInfoDto> paramInfo = this.paramInfo;
        for (ParameterInfoDto info : tree.paramInfo.values())
            paramInfo.put(info.name, info);

        remap(tree.root, id2id, big2big);
    }

    private void remap(Hotspot node, HashMap<Integer, Integer> id2id, HashMap big2big) {
        Integer newId = id2id.get(node.id);
        if (newId != null)
            node.id = newId;

        if (node.children != null)
            for (Hotspot child : node.children)
                remap(child, id2id, big2big);

        final Map<HotspotTag, HotspotTag> tags = node.tags;
        if (tags == null || tags.isEmpty()) return;

        Map<HotspotTag, HotspotTag> newTags = new HashMap<HotspotTag, HotspotTag>();

        for (HotspotTag tag : tags.values()) {
            newId = id2id.get(tag.id);
            if (newId != null)
                tag.id = newId;
            final Object value = tag.value;
            if (value instanceof BigDedupParamKey || value instanceof BigParamKey)
                tag.value = big2big.get(value);
            newTags.put(tag, tag);
        }

        node.tags = newTags;
    }
}
