package com.netcracker.profiler.agent;

import java.util.List;

public interface EnhancementRegistry {
    // We do not want java agent to import asm.* classes.
    List/*<FilteredEnhancer>*/ getEnhancers(String className);

    void addEnhancer(String className, Object filteredEnhancer);

    Object getFilter(String name);

    void addFilter(String name, Object filter);
}
