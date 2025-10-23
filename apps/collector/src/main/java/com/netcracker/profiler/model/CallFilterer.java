package com.netcracker.profiler.model;

public interface CallFilterer<T> {
    /**
     * filter operator for Collection utils: does the record match criteria?
     * @param call parsed information about call (`Call` or `CallRecord`)
     * @return true if we should keep this call in results
     */
    boolean filter(T call);
}
