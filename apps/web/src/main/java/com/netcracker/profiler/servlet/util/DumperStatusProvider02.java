package com.netcracker.profiler.servlet.util;

import com.netcracker.profiler.agent.Bootstrap;
import com.netcracker.profiler.agent.DumperPlugin;
import com.netcracker.profiler.agent.DumperPlugin_02;

public class DumperStatusProvider02 extends DumperStatusProvider {
    final DumperPlugin_02 dumper = (DumperPlugin_02) Bootstrap.getPlugin(DumperPlugin.class);

    public void update() {
        isStarted = dumper.isStarted();
        activeTime = System.currentTimeMillis() - dumper.getDumperStartTime();
        numberOfRestarts = dumper.getNumberOfRestarts();
        writeTime = dumper.getWriteTime();
        writtenBytes = dumper.getWrittenBytes();
        writtenRecords = dumper.getWrittenRecords();
        currentRoot = dumper.getCurrentRoot();
    }
}
