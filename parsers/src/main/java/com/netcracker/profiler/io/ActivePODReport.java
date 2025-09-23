package com.netcracker.profiler.io;

import java.util.*;

public class ActivePODReport {

    String namespace;
    String serviceName;
    String podName;

    long activeSinceMillis = Long.MAX_VALUE;
    long firstSampleMillis = Long.MAX_VALUE;
    long lastSampleMillis = Long.MIN_VALUE;

    long dataAtStart = 0;
    long dataAtEnd = 0;

    Map<Long, Long> lastAndSecondLastData = new TreeMap<>();

    Float currentBitrate = 0f;

    List<DownloadOptions> downloadOptions = new ArrayList<>();

    boolean onlineNow;

    List<HeapDumpInfo> heapDumps = new ArrayList<>();

    public ActivePODReport(String podName) {
        this.podName = podName;
    }

    public void acceptActiveSinceStat(long statActiveSinceMillis){
        this.activeSinceMillis = Math.min(this.activeSinceMillis, statActiveSinceMillis);
    }

    public void accepSampleMillis(long statSampleMIllis){
        this.firstSampleMillis = Math.min(this.firstSampleMillis, statSampleMIllis);
        this.lastSampleMillis = Math.max(this.lastSampleMillis, statSampleMIllis);
    }

    public void acceptDataStat(long statDate, long statData){
        this.dataAtStart = Math.min(this.dataAtStart, statData);
        this.dataAtEnd = Math.max(this.dataAtEnd, statData);

        lastAndSecondLastData.put(statDate, statData);
        while(lastAndSecondLastData.size() > 2){
            lastAndSecondLastData.remove(lastAndSecondLastData.keySet().iterator().next());
        }
    }

    public void acceptStreamReport(ActivePODStreamReport streamReport){
        this.activeSinceMillis = Math.min(this.activeSinceMillis, streamReport.activeSinceMillis);
        this.firstSampleMillis = Math.min(this.firstSampleMillis, streamReport.firstSampleMillis);
        this.lastSampleMillis = Math.max(this.lastSampleMillis, streamReport.lastSampleMillis);

        this.currentBitrate += streamReport.currentBitrate;

        if("gc".equals(streamReport.streamName)){
            downloadOptions.add(new DownloadOptions("gc", "/esc/download"));
        }
        if("top".equals(streamReport.streamName)) {
            downloadOptions.add(new DownloadOptions("top", "/esc/download"));
        }
        if("td".equals(streamReport.streamName)){
            downloadOptions.add(new DownloadOptions("td", "/esc/download"));
        }
    }

    public void addGoProfileType(String type) {
        downloadOptions.add(new DownloadOptions(type, "/esc/v1/download"));
    }

    public String getPodName() {
        return podName;
    }

    public void setPodName(String podName) {
        this.podName = podName;
    }

    public long getActiveSinceMillis() {
        return activeSinceMillis;
    }

    public void setActiveSinceMillis(long activeSinceMillis) {
        this.activeSinceMillis = activeSinceMillis;
    }

    public long getFirstSampleMillis() {
        return firstSampleMillis;
    }

    public void setFirstSampleMillis(long firstSampleMillis) {
        this.firstSampleMillis = firstSampleMillis;
    }

    public long getLastSampleMillis() {
        return lastSampleMillis;
    }

    public void setLastSampleMillis(long lastSampleMillis) {
        this.lastSampleMillis = lastSampleMillis;
    }

    public long getDataAtStart() {
        return dataAtStart;
    }

    public void setDataAtStart(long dataAtStart) {
        this.dataAtStart = dataAtStart;
    }

    public long getDataAtEnd() {
        return dataAtEnd;
    }

    public void setDataAtEnd(long dataAtEnd) {
        this.dataAtEnd = dataAtEnd;
    }

    public void addDataAtEnd(long dataAtEnd) {
        this.dataAtEnd += dataAtEnd;
    }

    public float getCurrentBitrate() {
        return currentBitrate;
    }

    public void setCurrentBitrate(float currentBitrate) {
        this.currentBitrate = currentBitrate;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public List<HeapDumpInfo> getHeapDumps() {
        return heapDumps;
    }

    public void setHeapDumps(List<HeapDumpInfo> heapDumps) {
        this.heapDumps = heapDumps;
    }

    public boolean isOnlineNow() {
        return onlineNow;
    }

    public void setOnlineNow(boolean onlineNow) {
        this.onlineNow = onlineNow;
    }

    public List<DownloadOptions> getDownloadOptions() {
        return downloadOptions;
    }

    private static class DownloadOptions {
        String typeName;
        String uri;

        private DownloadOptions(String typeName, String uri) {
            this.typeName = typeName;
            this.uri = uri;
        }

        private String getTypeName() {
            return typeName;
        }

        private String getUri() {
            return uri;
        }
    }
}
