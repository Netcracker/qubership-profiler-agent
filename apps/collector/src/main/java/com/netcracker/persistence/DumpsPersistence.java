package com.netcracker.persistence;

import java.util.List;

import com.netcracker.common.models.TimeRange;
import com.netcracker.common.models.pod.Dumps;
import com.netcracker.persistence.adapters.cloud.cdt.CloudDumpEntity;
import com.netcracker.persistence.adapters.cloud.cdt.dumps.CloudDumpsEntity;
import com.netcracker.persistence.adapters.cloud.cdt.dumps.CloudHeapDumpsEntity;

public interface DumpsPersistence {

    List<CloudDumpEntity> searchDumps(List<String> podIds, TimeRange range, String dumpType);

    List<CloudHeapDumpsEntity> getHeapDumps(TimeRange range);

    List<CloudDumpsEntity> getDumpObjects(String namespace, String service, TimeRange range);

    int getDumpsCount(String dumpType);

    Dumps getDump(String id, String dumpType);

}
