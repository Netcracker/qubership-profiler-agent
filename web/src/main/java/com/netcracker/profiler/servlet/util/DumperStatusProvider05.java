package com.netcracker.profiler.servlet.util;

import com.netcracker.profiler.agent.Bootstrap;
import com.netcracker.profiler.agent.DumperPlugin;
import com.netcracker.profiler.agent.DumperPlugin_05;

public class DumperStatusProvider05 extends DumperStatusProvider04 {
    DumperPlugin_05 dumper = (DumperPlugin_05) Bootstrap.getPlugin(DumperPlugin.class);

    @Override
    public void update() {
        super.update();
        fileRead = dumper.getFileRead();
        writtenBytes = dumper.getFileWritten();
    }
}
