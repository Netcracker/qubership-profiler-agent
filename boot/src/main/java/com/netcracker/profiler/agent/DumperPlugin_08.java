package com.netcracker.profiler.agent;

/**
 * @author logunov
 */
public interface DumperPlugin_08 extends DumperPlugin_07 {
    long getArchiveSize();
    void forceRescanDumpDir();
}
