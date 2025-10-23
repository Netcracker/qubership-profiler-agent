package com.netcracker.persistence.adapters.cloud;

import com.netcracker.common.PersistenceType;
import com.netcracker.common.models.SuspendRange;
import com.netcracker.common.models.TimeRange;
import com.netcracker.common.models.meta.DictionaryModel;
import com.netcracker.common.models.meta.ParamsModel;
import com.netcracker.common.models.meta.SuspendHickup;
import com.netcracker.common.models.pod.PodIdRestart;
import com.netcracker.persistence.PodsMetaPersistence;
import com.netcracker.persistence.adapters.cloud.cdt.CloudDictionaryEntity;
import com.netcracker.persistence.adapters.cloud.cdt.CloudParamsEntity;
import com.netcracker.persistence.adapters.cloud.cdt.CloudSuspendEntity;
import com.netcracker.persistence.adapters.cloud.dao.CloudDictionaryDao;
import com.netcracker.persistence.adapters.cloud.dao.CloudParamsDao;
import com.netcracker.persistence.adapters.cloud.dao.CloudSuspendDao;
import com.netcracker.persistence.op.Operation;
import io.quarkus.arc.lookup.LookupIfProperty;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.List;

/**
 * 99%* of the time, it's a ridiculous micro-optimization that people have some vague idea makes things 'better'.
 * This completely ignores the fact that, unless you're in an extremely tight and busy loop over millions of SQL
 * results all the time, which is hopefully rare, you'll never notice it. For everyone who's not doing that,
 * the developer time cost of maintaing, updating, and fixing bugs in the column indexing are far greater
 * than the incremental cost of hardware for your infinitesimally-worse-performing application.
 * <p>
 * Don't code optimizations like this in. Code for the person maintaining it. Then observe, measure, analyse, and
 * optimize. Observe again, measure again, analyse again, and optimize again.
 * <p>
 * Optimization is pretty much the last step in development, not the first.
 */

@LookupIfProperty(name = "service.persistence", stringValue = PersistenceType.CLOUD)
@ApplicationScoped
public class CloudMetaService implements PodsMetaPersistence {

    @Inject
    CloudDictionaryDao cloudDictionaryDao;

    @Inject
    CloudSuspendDao cloudSuspendDao;

    @Inject
    CloudParamsDao cloudParamsDao;

    @Override
    public List<ParamsModel> getParams(PodIdRestart pod) {
        return cloudParamsDao
                .find(pod.podId())
                .stream()
                .map(p -> p.toModel(pod))
                .toList();
    }

    @Override
    public List<DictionaryModel> getDictionary(PodIdRestart pod) {
        return cloudDictionaryDao
                .find(pod.podId(), pod.restartTime())
                .stream()
                .map(d -> d.toModel(pod))
                .toList();
    }

    @Override
    public List<DictionaryModel> getDictionary(PodIdRestart pod, List<Integer> ids) {
        if (ids == null || ids.isEmpty()) {
            return getDictionary(pod);
        }
        return cloudDictionaryDao
                .find(pod.podId(), pod.restartTime(), ids)
                .stream()
                .map(d -> d.toModel(pod))
                .toList();
    }

    @Override
    public SuspendRange getSuspends(PodIdRestart pod, TimeRange time) {
        var arr = cloudSuspendDao.find(time.days(), pod.podId(), pod.restartTime(), time.from(), time.to());
        var range = new SuspendRange();
        for (var item : arr) {
            for (var e : item.suspendTime().entrySet()) {
                range.add(item.curTime().toEpochMilli() + e.getKey(), e.getValue());
            }
        }
        return range;
    }

    @Override
    public Operation save(ParamsModel toSave) {
        cloudParamsDao.insert(CloudParamsEntity.prepare(toSave));
        cloudParamsDao.commit();
        return null;
    }

    @Override
    public Operation save(DictionaryModel toSave) {
        cloudDictionaryDao.insert(CloudDictionaryEntity.prepare(toSave));
        cloudDictionaryDao.commit();
        return null;
    }

    @Override
    public Operation save(SuspendHickup toSave) {
        cloudSuspendDao.insert(CloudSuspendEntity.prepare(toSave));
        cloudSuspendDao.commit();
        return null;
    }
}
