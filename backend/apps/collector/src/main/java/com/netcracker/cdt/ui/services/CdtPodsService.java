package com.netcracker.cdt.ui.services;

import com.netcracker.cdt.ui.models.UiServiceConfig;
import com.netcracker.common.Time;
import com.netcracker.common.models.TimeRange;
import com.netcracker.common.models.pod.IPodFilter;
import com.netcracker.common.models.pod.Namespace;
import com.netcracker.common.models.pod.PodIdRestart;
import com.netcracker.common.models.pod.PodInfo;
import com.netcracker.common.models.pod.stat.HeapDump;
import com.netcracker.common.models.pod.stat.PodReport;
import com.netcracker.common.models.pod.stat.PodRestartStat;
import com.netcracker.common.utils.DB;
import com.netcracker.persistence.PersistenceService;
import com.netcracker.persistence.adapters.cloud.cdt.dumps.CloudDumpPodsEntity;
import io.quarkiverse.bucket4j.runtime.RateLimited;
import io.quarkus.arc.lookup.LookupIfProperty;
import io.quarkus.logging.Log;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.*;

@LookupIfProperty(name = "service.type", stringValue = "ui")
@ApplicationScoped
public class CdtPodsService {

    @Inject
    Time time;

    @Inject
    PersistenceService persistence;
    @Inject
    UiServiceConfig config;

    @PostConstruct
    public void init() {
    }

    @DB
    public Collection<Namespace> getNamespaces() {
        return persistence.pods.getNamespaces();
    }

    @DB
    public Collection<PodInfo> getActivePods(TimeRange timeRange, IPodFilter filter) {
        return persistence.pods.getActivePods(timeRange, filter);
    }

    @DB
    public Collection<PodReport> getPodsInfo(TimeRange range, String podFilterQuery) {
        var pods = persistence.pods.activePods(range);
        // TODO query to IPodFilter

        Collection<PodReport> result = podsInfoReport(range, pods);
        return result;
    }

    @DB
    public Collection<CloudDumpPodsEntity> getDumpPods(String namespace, String service, TimeRange range) {
        return persistence.pods.findDumpPods(namespace, service, range);
    }

    @RateLimited(bucket = "pods")
    @DB
    Collection<PodReport> podsInfoReport(TimeRange timeRange, List<PodInfo> activePods) {
        Set<PodIdRestart> pods = new TreeSet<>();

        Log.infof("found %d active pods in time range [%s, %s]", activePods.size(), timeRange.from(), timeRange.to());
        activePods.forEach(p -> {
            pods.add(p.restartId());
        });

        var lastStat = persistence.pods.getLatestPodsStatistics(pods, timeRange.to());
        Log.infof("found %d latest stats for pods until [%s]", lastStat.size(), timeRange.to());

        var firstStat = persistence.pods.getLatestPodsStatistics(pods, timeRange.from().plusSeconds(70));
        Log.infof("found %d latest stats for pods until [%s]", lastStat.size(), timeRange.from());

        // TODO add stats for all prods without `firstStat` (at their restart time)

        List<PodRestartStat> stat = new ArrayList<>();
        stat.addAll(firstStat);
        stat.addAll(lastStat);

        Map<String, PodReport> reportByPods = groupStatsToReport(activePods, stat);
        Collection<PodReport> data = reportByPods.values();
        Log.infof("send %d reports for pods", data.size());
        return data;
    }


    Map<String, PodReport> groupStatsToReport(List<PodInfo> activePods, List<PodRestartStat> stat) {
        Map<String, PodInfo> info = new HashMap<>();
        for (var p : activePods) {
            info.put(p.restartId().podId(), p);
        }

        Map<String, PodReport> reportByPods = new LinkedHashMap<>();
        for (var s : stat) {
            var podId = s.pod().podId();
            if (!reportByPods.containsKey(podId)) {
                var pod = info.get(podId);
                if (pod != null) {
                    var r = new PodReport(pod);
                    reportByPods.put(podId, r);
                }
            }
            PodReport report = reportByPods.get(podId);
            if (report != null) {
                report.accumulate(s);
                reportByPods.put(s.pod().podName(), report);
            } else {
                Log.warnf("could not find info for pod %s", podId);
            }
        }
        Log.infof("grouped %d reports", reportByPods.size());
        return reportByPods;
    }

    @DB
    public Collection<HeapDump> getHeapDumps(TimeRange range, IPodFilter filter) {
        var pods = persistence.pods.getActiveDumpPods(range, filter);
        List<String> podIds = new ArrayList<>();
        for (var p : pods) {
            podIds.add(p.getPodId());
        }

        var heaps = persistence.dumps.searchDumps(podIds, range, "heap");
        Log.infof("found %d heap dumps for %d pods", heaps.size(), podIds.size());

        var list = heaps.stream().map(h -> {
            var pod = pods.stream().filter(p -> p.getPodId().equals(h.podId())).findFirst().orElse(null);
            if (pod == null) {
                return null;
            }
            return new HeapDump(pod, h.id(), h.creationTime(), h.fileSize(), h.fileSize());
        }).filter(Objects::nonNull).sorted().toList();
        return list;
    }
}
