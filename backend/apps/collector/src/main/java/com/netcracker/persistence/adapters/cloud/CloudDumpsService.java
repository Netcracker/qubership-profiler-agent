package com.netcracker.persistence.adapters.cloud;

import com.netcracker.common.PersistenceType;
import com.netcracker.common.models.TimeRange;
import com.netcracker.common.models.pod.Dumps;
import com.netcracker.persistence.DumpsPersistence;
import com.netcracker.persistence.adapters.cloud.cdt.CloudDumpEntity;
import com.netcracker.persistence.adapters.cloud.cdt.dumps.CloudDumpsEntity;
import com.netcracker.persistence.adapters.cloud.cdt.dumps.CloudHeapDumpsEntity;
import com.netcracker.persistence.adapters.cloud.dao.CloudDumpDao;
import com.netcracker.persistence.adapters.cloud.dao.dumps.CloudDumpObjectsDao;
import com.netcracker.persistence.adapters.cloud.dao.dumps.CloudHeapDumpsDao;
import io.quarkus.arc.lookup.LookupIfProperty;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.List;

@LookupIfProperty(name = "service.persistence", stringValue = PersistenceType.CLOUD)
@ApplicationScoped
public class CloudDumpsService implements DumpsPersistence {

    @Inject
    CloudDumpDao cloudDumpDao;

    @Inject
    CloudHeapDumpsDao cloudHeapDumpsDao;

    @Inject
    CloudDumpObjectsDao cloudDumpObjectsDao;

    @Override
    public List<CloudDumpEntity> searchDumps(List<String> podIds, TimeRange range, String dumpType) {
        return cloudDumpDao.find(podIds, range.from(), range.to(), dumpType);
    }

    @Override
    public List<CloudHeapDumpsEntity> getHeapDumps(TimeRange range) {
        return cloudHeapDumpsDao.find(range.from(), range.to());
    }

    @Override
    public List<CloudDumpsEntity> getDumpObjects(String namespace, String service, TimeRange range) {
        return cloudDumpObjectsDao.find2(namespace, service, range.from(), range.to());
    }

    @Override
    public int getDumpsCount(String dumpType) {
        return cloudDumpDao.count(dumpType);
    }

    @Override
    public Dumps getDump(String id, String dumpType) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getDump'");
    }

}
