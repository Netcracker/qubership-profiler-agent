package com.netcracker.profiler.client;

import com.netcracker.profiler.agent.DumperCollectorClient;
import com.netcracker.profiler.agent.DumperCollectorClientFactory;
import com.netcracker.profiler.agent.DumperRemoteControlledStream;
import com.netcracker.profiler.agent.ESCLogger;

public class CollectorClientFactory implements DumperCollectorClientFactory {
    private static final ESCLogger log = ESCLogger.getLogger(CollectorClientFactory.class);

    private static volatile DumperCollectorClientFactory INSTANCE;

    public static DumperCollectorClientFactory instance() {
        if(INSTANCE == null) {
            synchronized (CollectorClientFactory.class) {
                if(INSTANCE == null) {
                    INSTANCE = new CollectorClientFactory();
                }
            }
        }
        return INSTANCE;
    }

    public static synchronized void injectFactory(DumperCollectorClientFactory override){
        log.info("CollectorClientFactory override: " + (override == null ? "reset" : override.getClass()));
        INSTANCE = override;
    }

    public DumperCollectorClient newClient(String host,
                                           int port,
                                           boolean ssl,
                                           String cloudNamespace,
                                           String microserviceName,
                                           String podName){
        log.fine("Initializing default client");
        return new DefaultCollectorClient(host, port, ssl, cloudNamespace, microserviceName, podName);
    }

    @Override
    public DumperRemoteControlledStream wrapOutputStream(int rollingSequenceId, String streamName, long rotationPeriod, long requiredRotationSize, DumperCollectorClient collectorClient) {
        return new RollingChunkStream(
                rollingSequenceId,
                streamName,
                rotationPeriod,
                requiredRotationSize,
                collectorClient
        );
    }
}
