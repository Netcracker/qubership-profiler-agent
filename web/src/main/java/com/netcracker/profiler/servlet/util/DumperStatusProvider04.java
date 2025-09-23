package com.netcracker.profiler.servlet.util;

import com.netcracker.profiler.agent.Bootstrap;
import com.netcracker.profiler.agent.DumperPlugin;
import com.netcracker.profiler.agent.DumperPlugin_04;

public class DumperStatusProvider04 extends DumperStatusProvider02 {
    DumperPlugin_04 dumper = (DumperPlugin_04) Bootstrap.getPlugin(DumperPlugin.class);

    @Override
    public void update() {
        super.update();
        bytesAllocated = dumper.getBytesAllocated();
        cpuTime = dumper.getCPUTime();
    }
}
