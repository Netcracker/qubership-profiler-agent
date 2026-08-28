package com.netcracker.profiler.io;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DictionaryStreamVisitorImpl implements IDictionaryStreamVisitor {

    private String podName;
    private final List<String> dictionaryModelsList = new ArrayList<>();
    private final Map<Integer, String> dictionaryModelsMap = new HashMap<>();

    public DictionaryStreamVisitorImpl(String podName) {
        this.podName = podName;
    }

    @Override
    public void visitDictionary(String tag) {
        dictionaryModelsList.add(tag);
    }

    @Override
    public List<String> getAndCleanDictionary() {
        List<String> dictionaryModels = new ArrayList<>(this.dictionaryModelsList);
        this.dictionaryModelsList.clear();
        return dictionaryModels;
    }

    @Override
    public void visitDictionary(int position, String tag) {
        dictionaryModelsMap.put(position, tag);
    }

    @Override
    public Map<Integer, String> getAndCleanDictionaryMap() {
        Map<Integer, String> dictionaryModels = new HashMap<>(this.dictionaryModelsMap);
        this.dictionaryModelsMap.clear();
        return dictionaryModels;
    }
}
