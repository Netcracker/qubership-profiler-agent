package org.qubership.profiler.agent.plugins;

import org.qubership.profiler.agent.Profiler;
import org.qubership.profiler.agent.ProfilerData;
import org.qubership.profiler.agent.ProfilerPluginLogger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProfilerPluginLoggerImpl implements ProfilerPluginLogger {
    private static final Logger logger = LoggerFactory.getLogger(ProfilerPluginLoggerImpl.class);

    @Override
    public void pluginError(Throwable t) {
        Profiler.event(t.toString(), ProfilerData.PARAM_PLUGIN_EXCEPTION);
        logger.error("Exception in profiler plugin", t);
    }
    public void error(String log) {
        logger.error(log);
    }

    @Override
    public void error(String log, Throwable t) {
        logger.error(log, t);
    }

    @Override
    public void warn(String log) {
        logger.warn(log);
    }
    @Override
    public void warn(String log, Throwable t) {
        logger.warn(log, t);
    }
    @Override
    public void info(String log) {
        logger.info(log);
    }

    public void debug(String log) {
        logger.debug(log);
    }
}
