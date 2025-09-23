package com.netcracker.profiler.io;

import java.util.List;

public interface IDictionaryStreamVisitor {

    void visitDictionary(String tag);

    List<String> getAndCleanDictionary();
}
