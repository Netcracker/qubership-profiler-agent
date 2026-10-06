package com.netcracker.profiler.agent;

public interface DumperPlugin_07 extends DumperPlugin_06 {

    public boolean gracefulShutdown();

    public boolean gracefulShutdown(long timeout);
}
