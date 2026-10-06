package com.netcracker.profiler.fetch;

import com.netcracker.profiler.dom.ProfiledTreeStreamVisitor;

import org.openjdk.jmc.common.item.IItemFilter;
import org.openjdk.jmc.flightrecorder.jdk.JdkFilters;

public class FetchJFRCpu extends FetchJFRDump {
    public FetchJFRCpu(ProfiledTreeStreamVisitor sv, String jfrFileName) {
        super(sv, jfrFileName);
    }

    @Override
    protected IItemFilter getIItemFilter() {
        return JdkFilters.EXECUTION_SAMPLE;
    }

}
