package com.netcracker.profiler.tags;

import java.util.List;

public interface Dictionary {

    String get(int id);

    List<String> asList();

    int size();

}
