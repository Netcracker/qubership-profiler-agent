package com.netcracker.profiler.io;

import java.util.ArrayList;
import java.util.List;

public class DictionaryStreamVisitorImpl implements IDictionaryStreamVisitor {
    private String podName;
    private List<String> dictionaryModels = new ArrayList<>();

    public DictionaryStreamVisitorImpl(String podName) {
        this.podName = podName;
    }


    @Override
    public void visitDictionary(String tag) {
        dictionaryModels.add(tag);
    }

    @Override
    public List<String> getAndCleanDictionary() {
        List<String> dictionaryModels = new ArrayList<>(this.dictionaryModels);

        this.dictionaryModels.clear();

        return dictionaryModels;
    }

}
