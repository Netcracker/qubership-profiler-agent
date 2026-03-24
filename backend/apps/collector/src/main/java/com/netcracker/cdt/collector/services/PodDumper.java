package com.netcracker.cdt.collector.services;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.netcracker.cdt.collector.common.CollectorConfig;
import com.netcracker.common.Time;
import com.netcracker.common.models.pod.PodIdRestart;
import com.netcracker.common.models.pod.PodStatus;
import com.netcracker.common.models.pod.stat.PodDataAccumulated;
import com.netcracker.common.models.pod.streams.StreamRegistry;
import com.netcracker.common.utils.DB;
import com.netcracker.persistence.op.Operation;
import com.netcracker.persistence.PersistenceService;
import io.quarkus.arc.lookup.LookupIfProperty;
import io.quarkus.cache.CacheInvalidate;
import io.quarkus.logging.Log;
import io.quarkus.scheduler.Scheduled;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static io.quarkus.scheduler.Scheduled.ConcurrentExecution.SKIP;

@LookupIfProperty(name = "service.type", stringValue = "collector")
@ApplicationScoped
//@Lock // fails with GZip synchronized ?
public class PodDumper {
    @Inject
    Time time;

    @Inject
    CollectorConfig config;

    @Inject
    PersistenceService persistence;

    protected static Cache<String, PodStatus> activePodsCache;

    protected static Map<PodIdRestart, PodDataAccumulated> accumulatedMap;

    protected static Instant prevPersistTime = Instant.EPOCH;

    @PostConstruct
    public void init() {
        activePodsCache = CacheBuilder.newBuilder().
                expireAfterAccess(config.getLogRetentionPeriod(), TimeUnit.MILLISECONDS).
                maximumSize(config.getMaxConnections() * 2L).build();
        accumulatedMap = new HashMap<>();
    }


    @Scheduled(every = "${pod.collector.stat.persist}", delayed = "${pod.collector.stat.persist.delay}", concurrentExecution = SKIP) // , skipExecutionIf = MyPredicate.class)
    @DB
    public void schedulePersistStat() {
        persistStat();
    }

    @CacheInvalidate(cacheName = "podsCache")
    public void persistStat() {
        var pods = new ArrayList<Operation>(); // should separate batches for different tables
        var podRestarts = new ArrayList<Operation>();
        for (var podStatus: activePodsCache.asMap().values()) {
            if (podStatus.isUpdatedAfter(prevPersistTime)) {
                pods.add(persistence.pods.updateLastActivePod(podStatus));
                podRestarts.add(persistence.pods.updateLastActivePodRestart(podStatus));
            }
        }
        if (!pods.isEmpty()) {
            Log.debugf("prepare to execute %d operations: update active for %d active pods [cron: %s]",
                    pods.size(), activePodsCache.size(), config.getPodStatCron());
            persistence.batch.execute(pods);
            persistence.batch.execute(podRestarts);
        }

        var stats = new ArrayList<Operation>();
        for (var e: accumulatedMap.entrySet()) {
            stats.add(persistence.pods.insertPodStatistics(e.getKey(), e.getValue()));
        }
        if (!stats.isEmpty()) {
            Log.debugf("prepare to execute %d operations: persist %d stat for %d active pods [cron: %s]",
                    stats.size(), accumulatedMap.size(), activePodsCache.size(), config.getPodStatCron());
            persistence.batch.execute(stats);
        }

        prevPersistTime = time.now();
        accumulatedMap.clear(); // TODO check thread-safe? @Lock?
    }

    /**
     * load old data, create new records on pods' connect
     */
    @DB
    public void initPod(PodStatus pod) {
        pod = persistence.pods.initializePod(pod);
        activePodsCache.put(pod.getPodId(), pod);
        persistStat();
    }

    public void received(StreamRegistry registry, int contentLength) {
        var pod = activePodsCache.getIfPresent(registry.podRestart().podId());
        if (pod != null) {
            pod.received(registry, contentLength);
            accumulatedMap.put(registry.podRestart(), pod.dataAccumulated());
        }
    }

    public void persisted(StreamRegistry registry, int length) {
        var pod = activePodsCache.getIfPresent(registry.podRestart().podId());
        if (pod != null) {
            pod.persisted(registry, length);
            accumulatedMap.put(registry.podRestart(), pod.dataAccumulated());
        }
    }
}
