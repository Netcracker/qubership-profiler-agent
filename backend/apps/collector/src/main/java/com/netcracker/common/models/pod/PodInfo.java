package com.netcracker.common.models.pod;

import com.google.common.collect.ImmutableMap;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static com.netcracker.common.models.pod.PodStatus.*;

/**
 * Similar to PodStatus, but immutable entity: information about latest/current pod restart for UI service
 */
public class PodInfo implements Comparable<PodInfo> {
    private static Map<String, String> GO_TAGS = Map.of(SERVICE_TYPE, GO_SERVICE_TYPE);
    private static Map<String, String> JAVA_TAGS = Map.of(SERVICE_TYPE, JAVA_SERVICE_TYPE);
    private PodIdRestart id;

    private Instant activeSince;
    private Instant lastActive;
    private Map<String, String> tags;

    PodInfo(PodIdRestart id, Instant activeSince, Instant lastActive, Map<String, String> tags) {
        this.id = id;
        this.activeSince = activeSince;
        this.lastActive = lastActive;
        this.tags = ImmutableMap.copyOf(tags);
    }

    public static PodInfo of(String namespace, String service, String originalPodName) {
        return of(namespace,service, originalPodName, JAVA_TAGS);
    }

    public static PodInfo of(String namespace, String service, String originalPodName, Map<String, String> tags) {
        var id = PodIdRestart.of(namespace, service, originalPodName);
        return new PodInfo(id, id.restartTime(), id.restartTime(), tags);
    }

    public static PodInfo ofGo(String namespace, String service, String goPodName, Instant activeSince, Instant lastActive) {
        var id = PodIdRestart.of(namespace, service, goPodName + "_" + activeSince.toEpochMilli());
        return new PodInfo(id, activeSince, lastActive, GO_TAGS);
    }

    public static PodInfo of(String namespace, String service, String originalPodName, Instant activeSince, Instant lastRestart, Instant lastActive) {
        var id = PodIdRestart.of(namespace, service, originalPodName);
        return new PodInfo(id, activeSince, lastActive, JAVA_TAGS);
    }

    public static PodInfo ofDb(String namespace, String service, String podName, String podId,
                               Instant activeSince, Instant lastRestart, Instant lastActive,
                               Map<String, String> tags) {
        var id = PodIdRestart.of(namespace, service, podName, podId, lastRestart);
        return new PodInfo(id, activeSince, lastActive, tags);
    }

    public static PodInfo empty(String podName, Long lastActiveTimeMs) { // for tests
        var id = new PodIdRestart(new PodName("unknown", "unknown", podName, podName), Instant.EPOCH);
        return new PodInfo(id, Instant.EPOCH, Instant.ofEpochMilli(lastActiveTimeMs), JAVA_TAGS);
    }

    public PodInfo updateActive(Instant lastActive) {
        return new PodInfo(id, activeSince(), lastActive, tags);
    }

    public PodInfo updateForRestart(Instant restartTime, Instant lastActive) {
        return new PodInfo(new PodIdRestart(id.pod(), restartTime), activeSince(), lastActive, tags);
    }

    public PodStatus asStatus() {
        return new PodStatus(id, activeSince, lastActive);
    }

    public boolean wasActive(Instant rangeFrom, Instant rangeTo) {
        if (rangeTo.isBefore(activeSince)) return false;
        if (rangeFrom.isAfter(lastActive)) return false;
        return true;
    }

    @Override
    public int compareTo(PodInfo o) {
        return id.podName().compareTo(o.id.podName());
    }


    // getters

    public PodIdRestart restartId() {
        return id;
    }

    public String getPodId() {
        return id.podId();
    }

    public PodName pod() {
        return id.pod();
    }

    public String podId() {
        return id.pod().name(); // without timestamp
    }

    public String podName() {
        return id.pod().podName();
    }

    public String screenName() {
        return id.oldPodName();
    }

    public String namespace() {
        return id.pod().namespace();
    }

    public String service() {
        return id.pod().service();
    }

    public Instant activeSince() {
        return activeSince;
    }

    public Instant restartTime() {
        return id.restartTime();
    }

    public Instant lastActive() {
        return lastActive;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    @Override
    public String toString() {
        return screenName();
    }

    public String oldPodName() {
        return id.oldPodName();
    }

    public List<String> getTagValues() {
        if (tags == null || !tags.containsKey(SERVICE_TYPE)) {
            return List.of(JAVA_SERVICE_TYPE);
        } else {
            return List.of(tags.get(SERVICE_TYPE));
        }
    }

}
