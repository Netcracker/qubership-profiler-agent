package com.netcracker.profiler.agent;

import java.io.File;
import java.util.List;

public interface DumperPlugin_01 extends DumperPlugin {
    public void reconfigure();

    public File getCurrentRoot();

    public List<String> getTags();
}
