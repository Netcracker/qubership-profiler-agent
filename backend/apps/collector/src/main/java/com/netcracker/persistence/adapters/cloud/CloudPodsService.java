package com.netcracker.persistence.adapters.cloud;

import com.netcracker.common.PersistenceType;
import com.netcracker.common.Time;
import com.netcracker.common.models.TimeRange;
import com.netcracker.common.models.pod.PodIdRestart;
import com.netcracker.common.models.pod.PodInfo;
import com.netcracker.common.models.pod.PodStatus;
import com.netcracker.common.models.pod.stat.PodDataAccumulated;
import com.netcracker.common.models.pod.stat.PodReport;
import com.netcracker.common.models.pod.stat.PodRestartStat;
import com.netcracker.persistence.PodsPersistence;
import com.netcracker.persistence.adapters.cloud.cdt.CloudPodRestartsEntity;
import com.netcracker.persistence.adapters.cloud.cdt.CloudPodStatisticsEntity;
import com.netcracker.persistence.adapters.cloud.cdt.CloudPodsEntity;
import com.netcracker.persistence.adapters.cloud.cdt.dumps.CloudDumpPodsEntity;
import com.netcracker.persistence.adapters.cloud.dao.CloudPodRestartsDao;
import com.netcracker.persistence.adapters.cloud.dao.CloudPodStatisticsDao;
import com.netcracker.persistence.adapters.cloud.dao.CloudPodsDao;
import com.netcracker.persistence.adapters.cloud.dao.dumps.CloudDumpPodsDao;
import com.netcracker.persistence.op.Operation;
import io.quarkus.arc.lookup.LookupIfProperty;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


@LookupIfProperty(name = "service.persistence", stringValue = PersistenceType.CLOUD)
@ApplicationScoped
public class CloudPodsService implements PodsPersistence {

    @Inject
    Time time;

    @Inject
    CloudPodsDao cloudPodsDao;

    @Inject
    CloudDumpPodsDao cloudDumpPodsDao;

    @Inject
    CloudPodRestartsDao cloudPodRestartsDao;

    @Inject
    CloudPodStatisticsDao cloudPodStatisticsDao;

    @Override
    public TimeRange activePeriod() {
        return time.ofLast(30, ChronoUnit.MINUTES);
    }

    // ------------------------------------------------------------------------------------
    //    collector

    @Override
    public PodStatus initializePod(PodStatus pod) {

        // load previous data (if available in case of restart/reconnect)
        var podData = cloudPodsDao.find(pod.namespace(), pod.service(), pod.podName());

        if (podData.isPresent()) {
            // override activeSince
            pod.overrideActiveSince(podData.get().activeSince());
            // load last stat
            var stat = cloudPodStatisticsDao.findLatestStat(pod.restartId().oldPodName(), pod.restartTime(), time.days(time.now()));
            if (stat.isPresent()) {
                var acc = PodDataAccumulated.of(stat.get().originalAccumulated(), stat.get().dataAccumulated());
                pod.overrideAccumulated(acc);
            }
            // restartTime/lastActive would be updated by persistStat (cron)
        } else {
            var cloudPods = new CloudPodsEntity(pod.restartId().oldPodName(), pod.namespace(), pod.service(), pod.podName(), pod.activeSince(), pod.restartTime(), pod.lastActive(), pod.tags());
            cloudPodsDao.insert(cloudPods);
            cloudPodsDao.commit();
        }

        var podRestart = new CloudPodRestartsEntity(pod.restartId().oldPodName(), pod.namespace(), pod.service(), pod.podName(), pod.restartTime(), pod.activeSince(), pod.lastActive());
        cloudPodRestartsDao.insert(podRestart);
        cloudPodRestartsDao.commit();

        return pod;
    }

    @Override
    public Operation updateLastActivePod(PodStatus pod) {

        String s = "PodStatus{" +
                "id=" + pod.podId() +
                ", originalPodName='" + pod.screenName() + '\'' +
                ", tags=" + pod.tags() +
                ", activeSince=" + pod.activeSince() +
                ", lastActive=" + pod.lastActive() +
                ", restartTime=" + pod.restartTime() +
                '}';

        Log.infof("update %s", s);

        // TODO: why we need to update restartTime?
        // TODO: maybe error here - podName instead of podId
        cloudPodsDao.update(pod.screenName(), pod.lastActive(), pod.restartTime());
        return Operation.empty();
    }

    @Override
    public Operation updateLastActivePodRestart(PodStatus pod) {
        cloudPodRestartsDao.update(pod.podId(), pod.lastActive());
        return Operation.empty();
    }

    // ------------------------------------------------------------------------------------
    //    common

    @Override
    public List<PodInfo> allServices() {
        return cloudPodsDao
                .findAllServices()
                .stream()
                .map(CloudPodsEntity::asPodInfo)
                .toList();
    }

    @Override
    public List<PodInfo> allPods() {
        return cloudPodsDao
                .find()
                .stream()
                .map(CloudPodsEntity::asPodInfo)
                .toList();
    }

    @Override
    public List<PodInfo> activePods(TimeRange range) {
        return cloudPodsDao
                .find(range.from(), range.to())
                .stream()
                .map(CloudPodsEntity::asPodInfo)
                .toList();
    }

    @Override
    public List<PodInfo> activeDumpPods(TimeRange range) {
        return cloudPodsDao
                .findDumpPods(range.from(), range.to())
                .stream()
                .map(CloudPodsEntity::asPodInfo)
                .toList();
    }

    @Override
    public List<PodInfo> activePods(TimeRange range, List<String> namespaces, List<String> services) {
        return cloudPodsDao
                .find(namespaces, services, range.from(), range.to())
                .stream()
                .map(CloudPodsEntity::asPodInfo)
                .toList();
    }

    @Override
    public List<CloudDumpPodsEntity> findDumpPods(String namespace, String service, TimeRange range) {
        return cloudDumpPodsDao
                .find(namespace, service, range.from(), range.to())
                .stream()
                .toList();
    }

    @Override
    public List<PodInfo> podRestarts(PodInfo pod, TimeRange range, int limit) {
        List<CloudPodRestartsEntity> list = cloudPodRestartsDao.find(pod.podId(), range.from(), range.to(), limit);
        var res = new ArrayList<PodInfo>(list.size());
        for (var pr : list) {
            if (pr.wasActive(range.from(), range.to())) {
                res.add(pod.updateForRestart(pr.restartTime(), pr.lastActive()));
            }
        }
        return res;
    }

    // TODO: NOT USED YET
    @Override
    public List<PodInfo> escAllPods() {
        Log.warnf("escAllPods");
        return List.of();
    }

    // ------------------------------------------------------------------------------------
    //    stats

    @Override
    public Operation insertPodStatistics(PodIdRestart pod, PodDataAccumulated accumulated) {
        var date = time.today();
        var curMinute = time.curMinute();
        cloudPodStatisticsDao.insert(CloudPodStatisticsEntity.prepare(date, curMinute, pod, accumulated));
        cloudPodStatisticsDao.commit();
        return Operation.empty();
    }

    @Override
    public List<PodRestartStat> getLatestPodsStatistics(Collection<PodIdRestart> pods, Instant to) {
        List<PodRestartStat> list = new ArrayList<>();
        for (var p : pods) {
            list.addAll(getLatestPodStatistics(p, to));
        }
        return list;
    }

    @Override
    public List<PodRestartStat> getLatestPodStatistics(PodIdRestart pod, Instant to) {
        // TODO: why do we use pod.podName() and not pod.podId() | why pod.podId() contains _timestamp when testing
        return cloudPodStatisticsDao
                .find(pod.podName(), to)
                .stream().map(a -> a.toModel(pod))
                .toList();
    }

    // TODO: NOT USED YET
    @Override
    public List<PodReport> getStatistics(TimeRange range, List<PodInfo> pods) {
        Log.warnf("getStatistics");
        return List.of();
    }
}
