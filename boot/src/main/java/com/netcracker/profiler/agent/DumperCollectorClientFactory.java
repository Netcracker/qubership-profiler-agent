package com.netcracker.profiler.agent;


public interface DumperCollectorClientFactory {
    DumperCollectorClient newClient(String host,
                              int port,
                              boolean ssl,
                              String cloudNamespace,
                              String microserviceName,
                              String podName);

    DumperRemoteControlledStream wrapOutputStream(int rollingSequenceId,
                                                  String streamName,
                                                  long rotationPeriod,
                                                  long requiredRotationSize,
                                                  DumperCollectorClient collectorClient
    );
}
