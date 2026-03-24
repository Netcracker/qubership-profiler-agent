package com.netcracker.profiler.sax.visitors;

import java.util.List;

public interface IDictionaryStreamVisitor {

    void visitDictionary(String tag);

    List<String> getAndCleanDictionary();
}
