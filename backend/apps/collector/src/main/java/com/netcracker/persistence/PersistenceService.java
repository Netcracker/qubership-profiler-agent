package com.netcracker.persistence;

import io.quarkus.logging.Log;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Singleton;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.io.IOException;

@Singleton
public class PersistenceService {

    @ConfigProperty(name = "service.persistence")
    String persistenceType;

    public final PodsPersistence pods;
    public final PodsMetaPersistence meta;
    public final StreamsPersistence streams;
    public final DumpsPersistence dumps;
    public final BatchPersistence batch;
    public final CallSequenceLoader cloud;
    public final CallsPersistence calls;

    public PersistenceService(Instance<PodsPersistence> pods,
                              Instance<PodsMetaPersistence> meta,
                              Instance<StreamsPersistence> streams,
                              Instance<DumpsPersistence> dumps,
                              Instance<BatchPersistence> batch,
                              Instance<CallSequenceLoader> cloud,                              
                              Instance<CallsPersistence> calls) {
        this.pods = pods.get();
        this.meta = meta.get();
        this.streams = streams.get();
        this.dumps = dumps.get();
        this.batch = batch.get();
        this.cloud = cloud == null ? null : cloud.get();
        this.calls = calls.get();
    }

    @PostConstruct
    public void init() throws IOException, InterruptedException {
        Log.infof("Init persistenceService, type: %s", getType());
    }

    public String getType() {
        return persistenceType;
    }
}
