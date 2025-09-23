package com.netcracker.profiler.agent;

public interface DumperPlugin_04 extends DumperPlugin_03 {
    /**
     * Returns number of bytes allocated in dumper thread
     * @return number of bytes allocated in dumper thread
     */
    public long getBytesAllocated();

    /**
     * Returns cpu time used by dumper thread
     * @return cpu time used by dumper thread
     */
    public long getCPUTime();
}
