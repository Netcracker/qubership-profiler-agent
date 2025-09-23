package com.netcracker.profiler.io;

public class HeapDumpInfo {
    long date;
    long bytes;
    String handle;

    public HeapDumpInfo(long date, long bytes, String handle) {
        this.date = date;
        this.bytes = bytes;
        this.handle = handle;
    }

    public long getDate() {
        return date;
    }

    public void setDate(long date) {
        this.date = date;
    }

    public long getBytes() {
        return bytes;
    }

    public void setBytes(long bytes) {
        this.bytes = bytes;
    }

    public String getHandle() {
        return handle;
    }

    public void setHandle(String handle) {
        this.handle = handle;
    }
}
