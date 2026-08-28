package com.netcracker.profiler.tags;

import java.util.ArrayList;
import java.util.List;

public class DictionaryList implements Dictionary {

    private final List<String> dictionary;

    public DictionaryList() {
        this.dictionary = new ArrayList<>();
    }

    public DictionaryList(List<String> dictionary) {
        this.dictionary = dictionary;
    }

    @Override
    public String get(int id) {
        return dictionary.get(id);
    }

    @Override
    public List<String> asList() {
        return dictionary;
    }

    @Override
    public int size() {
        return dictionary.size();
    }
}
