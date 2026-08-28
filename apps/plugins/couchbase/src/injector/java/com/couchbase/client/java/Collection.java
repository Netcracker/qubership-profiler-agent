package com.couchbase.client.java;

import com.netcracker.profiler.agent.Profiler;

public class Collection {

    public native String name();

    public void dumpName$profiler() {
        Profiler.event(this.name(), "collection.name");
    }

}
