package com.netcracker.common.models.pod.stat;

import com.netcracker.common.Time;
import com.netcracker.common.models.IStreamType;
import com.netcracker.common.models.StreamType;
import com.netcracker.common.models.TimeRange;
import com.netcracker.common.models.pod.PodInfo;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class PodReport {
    private final PodInfo pod;

    public long firstSampleMillis = Long.MAX_VALUE;
    public long lastSampleMillis = Long.MIN_VALUE;
    public PodDataAccumulated dataAtStart = PodDataAccumulated.empty();
    public PodDataAccumulated dataAtEnd = PodDataAccumulated.empty();

    public PodReport(PodInfo pod) {
        this.pod = pod;
    }

    public String namespace() {
        return pod.namespace();
    }

    public String service() {
        return pod.service();
    }

    public String podName() {
        return pod.podName();
    }

    public String oldPodName() {
        return pod.oldPodName();
    }

    public List<String> getTagValues() {
        return pod.getTagValues();
    }

    public Instant restartTime() {
        return pod.restartTime();
    }

    public Instant activeSince() {
        return pod.activeSince();
    }

    public Instant lastActive() {
        return pod.lastActive();
    }

    public void accumulate(long collectTimeFrom, long collectTimeTo, IStreamType streamType, long dataAccumulated){ // for go collector
        firstSampleMillis = collectTimeFrom;
        lastSampleMillis = collectTimeTo;
        dataAtEnd.append(streamType, true, dataAccumulated);
        dataAtEnd.append(streamType, false, dataAccumulated);
    }
    public void accumulate(PodRestartStat stat){
        long sampleTs = Math.min(System.currentTimeMillis(), stat.curTime().toEpochMilli());

        firstSampleMillis = Math.min(firstSampleMillis, sampleTs);
        lastSampleMillis = Math.max(lastSampleMillis, sampleTs);

        dataAtStart.min(stat.accumulated());
        dataAtEnd.max(stat.accumulated());
    }

    public TimeRange screenRange(Time time) {
        long from = Math.max(firstSampleMillis - 1, pod.restartTime().toEpochMilli());
        long to = Math.min(lastSampleMillis + 1, pod.lastActive().toEpochMilli());
        if (from == to) { // single snapshot
            to = time.now().toEpochMilli(); // till curTime
        }
        return TimeRange.ofEpochMilli(from, to);
    }

    public boolean isOnlineNow(Time time) {
        return Math.abs(time.now().toEpochMilli() - lastActive().toEpochMilli()) < 5 * 60 * 1000; // 5min
    }

    public boolean hasGC() {
        return has(StreamType.GC);
    }

    public boolean hasTops() {
        return has(StreamType.TOP);
    }

    public boolean hasTD() {
        return has(StreamType.TD);
    }

    public boolean has(StreamType type) {
        return dataAtStart.has(type) || dataAtEnd.has(type) && diff(type) != null;
    }

    public BlobSize diff(StreamType type) {
        var end = dataAtEnd.map().get(type);
        var start = dataAtStart.map().get(type);
        if (end == null) return start;
        if (start == null) return end;
        return BlobSize.diff(start, end);
    }

    public boolean isEmpty() { // no difference in downloadable stats (TD, TOP, GC)
        var diffGc = diff(StreamType.GC);
        var diffTop = diff(StreamType.TOP);
        var diffTd = diff(StreamType.TD);
        return diffGc == null && diffTop == null && diffTd == null;
    }

    public List<StreamType> dumps() {
        var list = new ArrayList<StreamType>(3);
        if (hasTops()) list.add(StreamType.TOP);
        if (hasTD()) list.add(StreamType.TD);
        if (hasGC()) list.add(StreamType.GC);
        return list;
    }

    public boolean oneRecord() {
        return lastSampleMillis - firstSampleMillis < 60;
    }

    public record HeapDumpInfo(long date, long bytes, String handle) {
    }

}
