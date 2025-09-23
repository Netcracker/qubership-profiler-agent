package com.netcracker.profiler.dom;

public class GanttInfo {
    public int id;
    public int emit;
    public long startTime;
    public long totalTime;
    public String fullRow;
    public int folderId;

    public GanttInfo(int id, int emit, long startTime, long totalTime, String fullRow, int folderId) {
        this.id = id;
        this.emit = emit;
        this.startTime = startTime;
        this.totalTime = totalTime;
        this.fullRow = fullRow;
        this.folderId = folderId;
    }

}
