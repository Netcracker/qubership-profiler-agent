package com.netcracker.profiler.servlet.util;

import java.io.File;

public class DumperStatusProvider {
    public boolean isStarted;
    public long activeTime;
    public int numberOfRestarts;
    public long writeTime;
    public long writtenBytes;
    public long writtenRecords;
    public File currentRoot;
    public long bytesAllocated;
    public long cpuTime;
    public long fileRead;
    public long uncompressedSize;
    public long archiveSize;

    public void update() {
    }

    public static final DumperStatusProvider INSTANCE = create();

    private static DumperStatusProvider create() {
        try {
            Class.forName("com.netcracker.profiler.agent.DumperPlugin_08");
            return (DumperStatusProvider) Class.forName("com.netcracker.profiler.servlet.util.DumperStatusProvider08").newInstance();
        } catch (Throwable t) {
            /* Ignore */
        }

        try {
            Class.forName("com.netcracker.profiler.agent.DumperPlugin_05");
            return (DumperStatusProvider) Class.forName("com.netcracker.profiler.servlet.util.DumperStatusProvider05").newInstance();
        } catch (Throwable t) {
            /* Ignore */
        }

        try {
            Class.forName("com.netcracker.profiler.agent.DumperPlugin_04");
            return (DumperStatusProvider) Class.forName("com.netcracker.profiler.servlet.util.DumperStatusProvider04").newInstance();
        } catch (Throwable t) {
            /* Ignore */
        }

        try {
            Class.forName("com.netcracker.profiler.agent.DumperPlugin_02");
            return (DumperStatusProvider) Class.forName("com.netcracker.profiler.servlet.util.DumperStatusProvider02").newInstance();
        } catch (Throwable t) {
            /* Ignore */
        }

        return new DumperStatusProvider();
    }
}
