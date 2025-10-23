package com.netcracker.persistence;

import com.netcracker.common.models.TimeRange;
import com.netcracker.common.models.pod.*;
import com.netcracker.common.models.pod.stat.PodDataAccumulated;
import com.netcracker.common.models.pod.stat.PodReport;
import com.netcracker.common.models.pod.stat.PodRestartStat;
import com.netcracker.common.utils.DB;
import com.netcracker.persistence.adapters.cloud.cdt.CloudPodsEntity;
import com.netcracker.persistence.adapters.cloud.cdt.dumps.CloudDumpPodsEntity;
import com.netcracker.persistence.op.Operation;
import io.quarkus.cache.CacheResult;
import io.quarkus.logging.Log;

import java.time.Instant;
import java.util.*;

public interface PodsPersistence {

    TimeRange activePeriod(); // from config: last 30 min, by default

    // ------------------------------------------------------------------------------------
    //    collector

    /*
      1. retrieve previous state (activeSince) if exists.
      2. create rows in pods and pod_restarts if not exists
         (also case: connection restart due collector problems, so this restartTime is already in DB)
     */
    PodStatus initializePod(PodStatus pod);

    Operation updateLastActivePod(PodStatus pod);

    Operation updateLastActivePodRestart(PodStatus pod);

    // ------------------------------------------------------------------------------------
    //    common

    List<PodInfo> allServices(); // all unique services (namespace + service_name)

    List<PodInfo> allPods(); // all registered pods

    List<PodInfo> activePods(TimeRange range);
    List<PodInfo> activeDumpPods(TimeRange range);

    //
    List<PodInfo> activePods(TimeRange range, List<String> namespaces, List<String> services);

    // /namespaces/{namespace}/services/{service}/dumps
    List<CloudDumpPodsEntity> findDumpPods(String namespace, String service, TimeRange range);

    List<PodInfo> podRestarts(PodInfo pod, TimeRange range, int limit);

    @DB
    default List<PodInfo> activePodsByFilter(IPodFilter podFilter) {
        return currentActivePods().stream().filter(podFilter).toList();
    }

    @CacheResult(cacheName = "podsCache") // TODO invalid cache after updates
    @DB
    default List<PodInfo> currentActivePods() {
        var range = activePeriod();
        var list = activePods(range);
        Log.infof("found %d active pods in current range: %s", list.size(), range);
        return list;
    }

    List<PodInfo> escAllPods();

    // ------------------------------------------------------------------------------------
    //    ui

    @DB
    default Collection<Namespace> getNamespaces() {
        SortedMap<String, Namespace> data = new TreeMap<>();
        Log.info("recalculate cache");
        var pods = allPods();
        for (var p: pods) {
//            if (!p.isValid()) continue;
            var n = data.computeIfAbsent(p.namespace(), i -> Namespace.create(p.namespace()));
            var s = n.computeIfAbsent(p.service(), i -> Service.create(p.namespace(), p.service()));
            s.put(p);
        }
        return data.values();
    }

    // /services
    @DB
    default Collection<PodInfo> getActivePods(TimeRange range, IPodFilter podFilter) {
        var activePods = activePods(range);
        var filtered = activePods.stream().filter(podFilter).toList();
        return filtered;
    }

    @DB
    default Collection<PodInfo> getActiveDumpPods(TimeRange range, IPodFilter podFilter) {
        var activePods = activeDumpPods(range);
        var filtered = activePods.stream().filter(podFilter).toList();
        return filtered;
    }

    @DB
    default Optional<PodInfo> findPod(TimeRange range, PodName pod) { // for tests
        return findPod(range, pod.namespace(), pod.service(), pod.podName()).stream().findFirst();
    }

    @DB
    default List<PodInfo> findPod(TimeRange range, String namespace, String service, String podName) {
        var pods = activePods(range, List.of(namespace), List.of(service));
        var filtered = pods.stream().filter(byPodName(namespace, service, podName)).toList();
        return filtered;
    }

    // ------------------------------------------------------------------------------------
    //    stats

    Operation insertPodStatistics(PodIdRestart pod, PodDataAccumulated accumulated);

    List<PodRestartStat> getLatestPodsStatistics(Collection<PodIdRestart> pods, Instant to);

    List<PodRestartStat> getLatestPodStatistics(PodIdRestart pod, Instant to);

    List<PodReport> getStatistics(TimeRange range, List<PodInfo> pods);

    // ------------------------------------------------------------------------------------
    //    utils

    static IPodFilter byPodName(String namespace, String service, String podName) {
        return p -> {
            // namespace and service -- mandatory
            if (namespace == null || !namespace.equals(p.namespace())) return false;
            if (service == null || !service.equals(p.service())) return false;
            // podName can be null -- means don't filter by podName
            if (podName != null && !podName.equals(p.podName())) return false;
            return true;
        };
    }

    static IPodFilter byOldPodName(String namespace, String oldPodName) { // old style podName: with _ts
        var id = PodIdRestart.of(namespace != null ? namespace : "unknown", oldPodName);
        if (oldPodName != null && id.isEmpty()) {
            return p -> false;
        }

        return p -> {
            if (namespace == null || !namespace.equals(p.namespace())) return false;
            if (oldPodName == null || !id.podName().equals(p.podName())) return false;
            return true;
        };
    }

}
