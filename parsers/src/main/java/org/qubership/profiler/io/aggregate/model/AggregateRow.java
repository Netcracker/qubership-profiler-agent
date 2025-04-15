package org.qubership.profiler.io.aggregate.model;

import java.util.ArrayList;
import java.util.List;

public class AggregateRow {
    private String title;
    private long count;
    private long duration;
    private double durationHours;
    private double durationPerExec;
    private int duration90thPercentile;
    private long cpuTime;
    private double cpuTimeHours;
    private double cpuTimePerExec;
    private long memory;
    private double memoryGb;
    private double memoryPerExecMb;
    private long queueing;
    private long suspension;
    private double suspensionPerExec;

    private List<Integer> allDurations = new ArrayList<>();

    public String getTitle() {
        return title;
    }



    public void setTitle(String title) {
        this.title = title;
    }



    public long getCount() {
        return count;
    }



    public void setCount(long count) {
        this.count = count;
    }



    public long getDuration() {
        return duration;
    }



    public void setDuration(long duration) {
        this.duration = duration;
    }



    public double getDurationHours() {
        return durationHours;
    }



    public void setDurationHours(double durationHours) {
        this.durationHours = durationHours;
    }



    public double getDurationPerExec() {
        return durationPerExec;
    }



    public void setDurationPerExec(double durationPerExec) {
        this.durationPerExec = durationPerExec;
    }



    public long getCpuTime() {
        return cpuTime;
    }



    public void setCpuTime(long cpuTime) {
        this.cpuTime = cpuTime;
    }



    public double getCpuTimeHours() {
        return cpuTimeHours;
    }



    public void setCpuTimeHours(double cpuTimeHours) {
        this.cpuTimeHours = cpuTimeHours;
    }



    public double getCpuTimePerExec() {
        return cpuTimePerExec;
    }



    public void setCpuTimePerExec(double cpuTimePerExec) {
        this.cpuTimePerExec = cpuTimePerExec;
    }



    public long getMemory() {
        return memory;
    }



    public void setMemory(long memory) {
        this.memory = memory;
    }



    public double getMemoryGb() {
        return memoryGb;
    }



    public void setMemoryGb(double memoryGb) {
        this.memoryGb = memoryGb;
    }



    public double getMemoryPerExecMb() {
        return memoryPerExecMb;
    }



    public void setMemoryPerExecMb(double memoryPerExecMb) {
        this.memoryPerExecMb = memoryPerExecMb;
    }



    public long getQueueing() {
        return queueing;
    }



    public void setQueueing(long queueing) {
        this.queueing = queueing;
    }



    public long getSuspension() {
        return suspension;
    }

    public void setSuspension(long suspension) {
        this.suspension = suspension;
    }

    public int getDuration90thPercentile() {
        return duration90thPercentile;
    }

    public void setDuration90thPercentile(int duration90thPercentile) {
        this.duration90thPercentile = duration90thPercentile;
    }

    public List<Integer> getAllDurations() {
        return allDurations;
    }

    public double getSuspensionPerExec() {
        return suspensionPerExec;
    }

    public void setSuspensionPerExec(double suspensionPerExec) {
        this.suspensionPerExec = suspensionPerExec;
    }

}
