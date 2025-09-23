package com.netcracker.profiler.dom;

import com.netcracker.profiler.configuration.ParameterInfoDto;

import java.util.*;

public class TagDictionary implements Cloneable {
    private Map<String, Integer> map;
    private ArrayList<String> methods;
    private Map<String, ParameterInfoDto> paramInfo = new HashMap<String, ParameterInfoDto>();

    private BitSet ids = new BitSet();

    public TagDictionary() {
        this(10);
    }

    public TagDictionary(int size) {
        map = new HashMap<String, Integer>((int) (size/0.75f));
        methods = new ArrayList<String>(size);
    }

    public synchronized int resolve(String methodName) {
        Integer methodId = map.get(methodName);
        if (methodId != null)
            return methodId;
        return createEntry(methodName);
    }

    private synchronized int createEntry(String methodName) {
        Integer id;
        id = methods.size();
        methods.add(methodName);
        map.put(methodName, id);
        ids.set(id);
        return id;
    }

    public synchronized void put(int id, String name) {
        ArrayList<String> methods = this.methods;

        methods.ensureCapacity(id + 1);
        for (int i = methods.size(); i <= id; i++)
            methods.add(null);

        methods.set(id, name);
        map.put(name, id);
        ids.set(id);
    }

    @Override
    public TagDictionary clone() {
        try {
            TagDictionary clone = (TagDictionary) super.clone();
            clone.map.putAll(map);
            clone.methods.addAll(methods);
            clone.paramInfo.putAll(paramInfo);
            clone.ids.or(ids);
            return clone;
        } catch (CloneNotSupportedException e) {
            throw new IllegalStateException("Should be able to clone TagDictionary", e);
        }
    }

    public String resolve(int id) {
        return methods.get(id);
    }

    public BitSet getIds() {
        return ids;
    }

    public ArrayList<String> getTags() {
        return methods;
    }

    public Map<Integer, Integer> merge(TagDictionary that) {
        if (that == this) return Collections.emptyMap();

        for (ParameterInfoDto info : that.getParamInfo().values()) {
            info(info);
        }

        Map<Integer, Integer> remap = new HashMap<Integer, Integer>();
        ArrayList<String> tags = that.getTags();
        int ourTags = methods.size();
        for (int i = 0; i < tags.size(); i++) {
            String s = tags.get(i);
            if (s == null)
                continue;
            if (i >= ourTags || !s.equals(methods.get(i))) {
                int newId = resolve(s);
                remap.put(i, newId);
                ids.set(newId);
            }
        }
        return remap;
    }

    public void info(ParameterInfoDto info) {
        this.paramInfo.put(info.name, info);
    }

    public Map<String, ParameterInfoDto> getParamInfo() {
        return paramInfo;
    }
}
