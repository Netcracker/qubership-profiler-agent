package com.netcracker.cdt.collector.tcp;

import com.netcracker.cdt.collector.common.CollectorConfig;
import com.netcracker.common.models.pod.PodStatus;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.quarkus.arc.lookup.LookupIfProperty;
import io.quarkus.logging.Log;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;

import static com.netcracker.cdt.collector.common.MetricsConst.CONNECTED_AGENTS;
import static com.netcracker.cdt.collector.common.MetricsConst.CONNECTED_AGENT_NAMESPACE;

@LookupIfProperty(name = "service.type", stringValue = "collector")
@ApplicationScoped
public class CollectorOrchestratorThread extends Thread {
    @Inject
    MeterRegistry registry;

    @Inject
    CollectorConfig config;

    private BlockingQueue<ProfilerAgentConnection> connections;

    private final Map<String, Gauge> CONNECTED_AGENTS_REPORTER_PER_NS = new ConcurrentHashMap<>();

    @PostConstruct
    void init() {
        connections = new ArrayBlockingQueue<>(config.getMaxConnections());

        Gauge.builder(CONNECTED_AGENTS, () -> connections.size())
                .description("Number of agents connected to the collector")
                .register(registry);
    }

    void addConnection(PodStatus pod, ProfilerAgentConnection pac) {
        Log.debugf("Trying to adding a connection to the pool for %s", pod.podName());
        if (!this.isAlive()) {
            throw new RuntimeException("Collector orchestrator thread died. Can not accept new connections");
        }
        if (!pod.isEmpty()) { // has original podName
            if (alreadyHasPod(pod.podId())) {
                deleteOldConnection(pod.podId());
            }

            // register metrics
            var namespace = pod.namespace();
            CONNECTED_AGENTS_REPORTER_PER_NS.computeIfAbsent(namespace, (ns) -> Gauge
                    .builder(CONNECTED_AGENT_NAMESPACE, getConnectionsByNamespace(namespace))
                    .description("Number of agents connected to the collector per namespace")
                    .tag("namespace", namespace)
                    .register(Metrics.globalRegistry)
            );
        }
        Log.debugf("Added a connection to the pool for %s", pod.podName());
        this.connections.add(pac);
    }

    private boolean alreadyHasPod(String podId) {
        for (var pod: connections) {
            if (podId.equals(pod.getPod().podId())) { // Pod reconnected
                return true;
            }
        }
        return false;
    }

    private void deleteOldConnection(String podId) {
        for (Iterator<ProfilerAgentConnection> it = connections.iterator(); it.hasNext(); ) {
            ProfilerAgentConnection oldPac = it.next();
            if (podId.equals(oldPac.getPod().podId())) {
                // Need to close the previous handle ASAP and free up resources for the new connection
                // Old pod is guaranteed to have finished writing at this point, delete everything in the cache
                oldPac.close("New pod connected: " + oldPac.getPod().restartId().oldPodName() + " | restart "+oldPac.getPod().restartTime());
                it.remove();
            }
        }
    }

    public Supplier<Number> getConnectionsByNamespace(String namespace) {
        return () -> connections.stream().filter(e -> e.getPod().namespace().equals(namespace)).count();
    }

    @Override
    public void run() {
        Log.infof("Started the orchestrator thread");
        while (isAlive()) {
            for (Iterator<ProfilerAgentConnection> it = connections.iterator(); it.hasNext(); ) {
                ProfilerAgentConnection pac = it.next();
                // TODO Even in case beingProcessed = true, kill it after 11 seconds of inactivity
                if (pac.timeToKill()) {
                    Log.debugf("Time to remove a pod connection '%s' from the pool", pac.getPod().podName());
                    try {
                        // Try to kill only once
                        it.remove();
                        if (!pac.shutdownComplete()) {
                            pac.kill();
                        }
                    } catch (Exception e) {
                        Log.error("failed to kill ProfilerAgentConfiguration: ", e);
                    }
                }
            }

            // wait 10 ms for the next packet of data
            LockSupport.parkNanos(10_000_000L);
        }
        Log.infof("Stopped the orchestrator thread");
    }

}
