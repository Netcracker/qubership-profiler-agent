package com.netcracker.common.models.pod;

import com.netcracker.cdt.collector.common.models.StreamInfoRequest;
import com.netcracker.common.models.StreamType;
import com.netcracker.common.models.pod.stat.PodDataAccumulated;
import com.netcracker.common.models.pod.streams.StreamRegistry;
import io.quarkus.logging.Log;
import org.apache.commons.lang.StringUtils;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Entity to persis data about pod during tcp connection (only for collector).
 * <p>
 * Three steps of lifetime:
 * 1. INITIALIZATION  - get info about NS/service/podName during COMMAND_GET_PROTOCOL_VERSION_V2
 * 2. LOADING         - loading information from DB (if exist)
 * 3. WORKING         - (main) update lastActive / dataAccumulated while receiving commands form connection
 */
public class PodStatus {
    public static final String PROTOCOL = "protocol";
    public static final String SERVICE_TYPE = "type";
    public static final String JAVA_SERVICE_TYPE = "java";
    public static final String GO_SERVICE_TYPE = "go";

    private PodIdRestart id;
    private String originalPodName;
    private Map<String, String> tags;

    private Instant activeSince;
    private Instant lastActive;
    private PodDataAccumulated accumulated;

    PodStatus(PodIdRestart id, Instant activeSince, Instant lastActive) {
        this.id = id;
        this.activeSince = activeSince;
        this.lastActive = lastActive;
        this.accumulated = PodDataAccumulated.empty();
        this.tags = new HashMap<>();
    }

    public static PodStatus empty(Instant curTime) {
        return new PodStatus(PodIdRestart.empty(), Instant.EPOCH, curTime);
    }

    // === INITIALIZATION

    public PodStatus setNamespace(String newName) {
        var pid = id.pod();
        pid = PodName.of(newName, pid.service(), pid.podName());
        id = new PodIdRestart(pid, id.restartTime());
        return this;
    }

    public PodStatus setMicroservice(String newName) {
        var pid = id.pod();
        pid = PodName.of(pid.namespace(), newName, pid.podName());
        id = new PodIdRestart(pid, id.restartTime());
        return this;
    }

    public PodStatus setPodName(String originalPodName) {
        var parsed = PodIdRestart.Parsed.fromOriginal(originalPodName);

        if (parsed.isValid()) {
            var pid = id.pod();
            pid = PodName.of(pid.namespace(), pid.service(), parsed.pod());
            id = new PodIdRestart(pid, parsed.start());
        } else {
            var pid = id.pod();
            pid = PodName.of(pid.namespace(), pid.service(), originalPodName);
            id = new PodIdRestart(pid, id.restartTime());
            Log.warnf("possible incorrect parsing of original podName ('%s'), " +
                            "got {service:'%s', pod: '%s', restart: %d}",
                originalPodName, parsed.service(), parsed.pod(), parsed.start().toEpochMilli());
        }
        this.originalPodName = originalPodName;

        if (activeSince.isAfter(Instant.EPOCH)) {
            activeSince = id.restartTime();
        } else {
            activeSince = id.restartTime();
        }

        return this;
    }


    public void setClientProtocolVersion(long clientProtocolVersion) {
        this.tags.put(PROTOCOL, Long.toString(clientProtocolVersion));
        this.tags.put(SERVICE_TYPE, JAVA_SERVICE_TYPE);
    }

    // === LOADING

    public void overrideActiveSince(Instant activeSince) { //  should call only once during init
        this.activeSince = activeSince;
    }

    public void overrideAccumulated(PodDataAccumulated acc) { //  should call only once during init
        this.accumulated = acc;
    }

    // === WORKING
    public StreamInfoRequest newStreamRequest(Instant t, StreamType stream, int requestedRollingSequenceId,
                                              boolean resetRequired, boolean forceRequestedRollingSequenceId ) {
        return new StreamInfoRequest(id, stream,
                requestedRollingSequenceId, resetRequired, forceRequestedRollingSequenceId, t, t);
    }

    public void touch(Instant t) {
        this.lastActive = t;
    }

    public void received(StreamRegistry registry, int contentLength) {
        registry.received(contentLength);
        this.accumulated.append(registry.stream(), true, contentLength);
        if (registry.isMetaStream()) {
            this.persisted(registry, contentLength); // data from special meta table will not be compressed
        }
    }

    public void persisted(StreamRegistry registry, int contentLength) {
        registry.persisted(contentLength);
        this.accumulated.append(registry.stream(), false, contentLength);
    }

    // getters

    public boolean isEmpty() {
        return StringUtils.isEmpty(originalPodName);
    }

    public boolean isUpdatedAfter(Instant lastUpdateTime) {
        return lastActive.isAfter(lastUpdateTime);
    }

    public boolean isValid() {
        if (isEmpty() || id.isEmpty()) {
            return false;
        }
        var parsed = PodIdRestart.Parsed.fromOriginal(originalPodName);
        if (!parsed.isValid()) {
            return false;
        }

        String serviceName = id.pod().service();
        var res = serviceName.equals(parsed.service());
        if (!res) {
            Log.tracef("possible incorrect parsing of original podName: " +
                        "incoming pod name: '%s', parsed service name: '%s', but already have service '%s'",
                originalPodName, parsed.service(), serviceName);
        }
        return res;
    }

    public String getPodId() {
        return id.podId();
    }

    public PodName pod() {
        return id.pod();
    }

    public PodIdRestart restartId() {
        return id;
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

    public Map<String, String> tags() {
        return tags;
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

    public PodDataAccumulated dataAccumulated() {
        return accumulated;
    }

    @Override
    public String toString() {
        return screenName();
    }

}
