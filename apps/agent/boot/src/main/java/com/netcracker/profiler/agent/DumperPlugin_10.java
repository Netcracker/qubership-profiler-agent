package com.netcracker.profiler.agent;

public interface DumperPlugin_10 extends DumperPlugin_09{
    void injectCollectorClientFactory(DumperCollectorClientFactory toInject);
    DumperCollectorClientFactory getCollectorClientFactory();
    boolean isInitialized();
}
