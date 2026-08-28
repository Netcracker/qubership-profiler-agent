package com.netcracker.profiler.io;

public class CallsFileHeader {
    private int fileFormat;
    private long startTime;

    public CallsFileHeader(int fileFormat, long startTime) {
        this.fileFormat = fileFormat;
        this.startTime = startTime;
    }

    public int getFileFormat() {
        return fileFormat;
    }

    public long getStartTime() {
        return startTime;
    }
}
