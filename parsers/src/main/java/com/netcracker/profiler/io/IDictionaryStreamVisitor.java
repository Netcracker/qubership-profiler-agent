package com.netcracker.profiler.io;

import java.util.List;
import java.util.Map;

public interface IDictionaryStreamVisitor {

    void visitDictionary(String tag);

    void visitDictionary(int position, String tag);

    List<String> getAndCleanDictionary();

    Map<Integer, String> getAndCleanDictionaryMap();
}
