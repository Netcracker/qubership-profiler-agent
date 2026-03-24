package com.netcracker.profiler.sax.visitors;

import java.util.ArrayList;
import java.util.List;

public class DictionaryStreamVisitorImpl implements IDictionaryStreamVisitor {
    private List<String> dictionaryModels = new ArrayList<>();

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
