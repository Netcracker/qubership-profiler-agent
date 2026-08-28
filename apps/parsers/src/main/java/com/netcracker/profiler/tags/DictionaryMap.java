package com.netcracker.profiler.tags;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DictionaryMap implements Dictionary {

    Map<Integer, String> dictionary;

    public DictionaryMap() {
        this.dictionary = new HashMap<>();
    }

    public DictionaryMap(Map<Integer, String> dictionary) {
        this.dictionary = dictionary;
    }

    @Override
    public String get(int id) {
        String tag;
        return (((tag = dictionary.get(id)) != null) || dictionary.containsKey(id)) ? tag : "{undefined}";
    }

    @Override
    public List<String> asList() {
        return new ArrayList<>(dictionary.values());
    }

    @Override
    public int size() {
        return dictionary.size();
    }
}
