package com.netcracker.profiler.servlet.util;

import com.netcracker.profiler.agent.Bootstrap;
import com.netcracker.profiler.agent.DumperPlugin;
import com.netcracker.profiler.agent.DumperPlugin_08;

/**
 * @author logunov
 */
public class DumperStatusProvider08 extends DumperStatusProvider06 {

    DumperPlugin_08 dumper = (DumperPlugin_08) Bootstrap.getPlugin(DumperPlugin.class);

    @Override
    public void update() {
        //if dumper plugin is absent, do not attempt to collect info
        if(dumper == null){
            return;
        }
        super.update();
        archiveSize = dumper.getArchiveSize();
    }
}
