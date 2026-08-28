package com.netcracker.profiler.servlet.util;

import com.netcracker.profiler.agent.Bootstrap;
import com.netcracker.profiler.agent.DumperPlugin;
import com.netcracker.profiler.agent.DumperPlugin_06;

public class DumperStatusProvider06 extends DumperStatusProvider05 {
    DumperPlugin_06 dumper = (DumperPlugin_06) Bootstrap.getPlugin(DumperPlugin.class);

    @Override
    public void update() {
        super.update();
        uncompressedSize = dumper.getUncompressedSize();
    }
}
