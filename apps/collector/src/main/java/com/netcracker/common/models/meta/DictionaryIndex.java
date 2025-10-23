package com.netcracker.common.models.meta;

import java.util.*;

public class DictionaryIndex implements Cloneable {
    private Map<String, Integer> map;
    private ArrayList<String> tags;  // at first, keep same tags ids from DB
    private Map<String, ParamsModel> paramInfo = new HashMap<>();

    private BitSet ids = new BitSet();

    public DictionaryIndex() {
        this(100);
    }

    protected DictionaryIndex(int size) {
        map = new HashMap<>((int) (size / 0.75f));
        tags = new ArrayList<>(size);
    }

    public BitSet getIds() {
        return ids;
    }

    public List<String> getTags() {
        return tags;
    }

    public Map<Integer, String> getTagMap() {
        var m = new HashMap<Integer, String>();
        for (int i = 0; i < tags.size(); i++) {
            String s = tags.get(i);
            if (s == null)
                continue;
            m.put(i, s);
        }
        return m;
    }

    public Map<String, ParamsModel> getParamInfo() {
        return paramInfo;
    }

    public void putDictionary(int id, String name) {
        ArrayList<String> methods = this.tags;

        methods.ensureCapacity(id + 1);
        for (int i = methods.size(); i <= id; i++)
            methods.add(null);

        methods.set(id, name);
        map.put(name, id);
        ids.set(id);
    }

    public void putParameter(ParamsModel info) {
        this.paramInfo.put(info.paramName(), info);
    }

    private int resolve(String methodName) {
        Integer methodId = map.get(methodName);
        if (methodId != null)
            return methodId;
        int id = tags.size();
        tags.add(methodName);
        map.put(methodName, id);
        ids.set(id);
        return id;
    }

    // should remap if merge calls tree from other pod (with different tagIds)
    public Map<Integer, Integer> mergeForRemap(DictionaryIndex that) {
        if (that == this) return Collections.emptyMap();

        for (var info : that.getParamInfo().values()) {
            putParameter(info);
        }

        Map<Integer, Integer> remapIds = new HashMap<Integer, Integer>();
        ArrayList<String> tags = that.tags;
        int ourTags = this.tags.size();
        for (int i = 0; i < tags.size(); i++) {
            String s = tags.get(i);
            if (s == null)
                continue;
            if (i >= ourTags || !s.equals(this.tags.get(i))) {
                int newId = resolve(s);
                remapIds.put(i, newId);
                ids.set(newId);
            }
        }
        return remapIds;
    }

    @Override
    public DictionaryIndex clone() {
        try {
            DictionaryIndex clone = (DictionaryIndex) super.clone();
            clone.map = new HashMap<>(map);
            clone.tags = new ArrayList<>(tags);
            clone.paramInfo = new HashMap<>(paramInfo);
            clone.ids = new BitSet();
            clone.ids.or(ids);
            return clone;
        } catch (CloneNotSupportedException e) {
            throw new IllegalStateException("Should be able to clone TagDictionary", e);
        }
    }
}
