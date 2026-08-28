package com.netcracker.profiler.agent;

public interface ProfilerPluginLogger {
    void pluginError(Throwable t);
    void error(String log);
    void error(String log, Throwable t);
    void warn(String log);
    void warn(String log, Throwable t);
    void info(String log);
    void debug(String log);

}
