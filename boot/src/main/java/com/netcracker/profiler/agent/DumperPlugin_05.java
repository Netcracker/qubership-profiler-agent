package com.netcracker.profiler.agent;

public interface DumperPlugin_05 extends DumperPlugin_04 {
    /**
     * Returns number of bytes read in dumper thread
     * @return number of bytes read in dumper thread
     */
    public long getFileRead();

    /**
     * Returns number of bytes written in dumper thread
     * @return number of bytes written in dumper thread
     */
    public long getFileWritten();
}
