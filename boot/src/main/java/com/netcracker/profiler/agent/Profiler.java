package com.netcracker.profiler.agent;

import org.xml.sax.SAXException;

import java.io.IOException;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;

public class Profiler {
    private static final ESCLogger logger = ESCLogger.getLogger(Profiler.class);
    static {
        startDumper();
    }

    private static void startDumper() {
        final DumperPlugin_02 dumper;
        try {
            dumper = (DumperPlugin_02) Bootstrap.getPlugin(DumperPlugin.class);
            if (dumper == null) {
                logger.severe("[Profiler] Unable to find Dumper in the class path");
                return;
            }
        } catch (Throwable e){
            logger.severe("[Profiler] Unable to find Dumper in the class path", e);
            return;
        }
        enter("void " + Profiler.class.getName() + ".startDumper() (Profiler.java:20) [profiler-runtime.jar]");

        dumper.newDumper(ProfilerData.dirtyBuffers, ProfilerData.emptyBuffers, ProfilerData.activeThreads);
        ProfilerTransformerPlugin plugin = Bootstrap.getPlugin(ProfilerTransformerPlugin.class);

        final ProfilerTransformerPlugin_01 transformerPlugin = (ProfilerTransformerPlugin_01) plugin;
        try {
            transformerPlugin.reloadClasses(null);
        } catch (IOException|SAXException|ParserConfigurationException e) {
            logger.severe("[Profiler] Unable to reload bootstrap classes", e);
        }
        exit(); // This is not in finally to ensure error message has better chances to be printed
    }

    public static LocalState getState() {
        return ProfilerData.localState.get();
    }

    public static void event(Object value, String name) {
        if (value == null) return;
        event(value, DumperConstants.DATA_TAG_RECORD | ProfilerData.resolveTag(name));
    }

    public static void event(Object value, int tagId) {
        getState().event(value, tagId);
    }

    public static void pluginException(Throwable t) {
        if(ProfilerData.pluginLogger != null) {
            ProfilerData.pluginLogger.pluginError(t);
        }
    }

    public static void logError(String log) {
        if(ProfilerData.pluginLogger != null) {
            ProfilerData.pluginLogger.error(log);
        }
    }

    public static void logError(String log, Throwable t) {
        if(ProfilerData.pluginLogger != null) {
            ProfilerData.pluginLogger.error(log, t);
        }
    }

    public static void logWarn(String log) {
        if(ProfilerData.pluginLogger != null) {
            ProfilerData.pluginLogger.warn(log);
        }
    }

    public static void logWarn(String log, Throwable t) {
        if(ProfilerData.pluginLogger != null) {
            ProfilerData.pluginLogger.warn(log, t);
        }
    }

    public static void logInfo(String log) {
        if(ProfilerData.pluginLogger != null) {
            ProfilerData.pluginLogger.info(log);
        }
    }

    public static void logDebug(String log) {
        if(ProfilerData.pluginLogger != null) {
            ProfilerData.pluginLogger.debug(log);
        }
    }

    public static void enter(String methodName) {
        enterWithDuration(methodName, -1);
    }

    public static void enterWithDuration(String methodName, long millisecondsPassed) {
        int methodId = ProfilerData.resolveTag(methodName) | DumperConstants.DATA_ENTER_RECORD;
        getState().enter(methodId, millisecondsPassed);
    }

    public static void enter(int methodId) {
        throw new RuntimeException("no longer supported");
    }

    public static LocalState enterReturning(int methodId) {
        final LocalState state = getState();
        state.enter(methodId);
        return state;
    }

    public static void exit() {
        getState().exit();
    }

    public static void markSystem() {
        getState().markSystem();
    }

    public static void threadExit() {
        final LocalState state = getState();
        if (ProfilerData.activeThreads.remove(state.thread) == null) return;
        // Do not use addEmptyBuffer since this would make synchronization much harder.
        // The problem with addEmpty happens when this thread adds to EmptyBuffer queue while Dumper steals data.
        // This might lead to inconsistency: buffer is acquired by another thread while the Dumper is writing it.
        ProfilerData.addDirtyBufferIfPossible(state.buffer);
    }

    public static final String EXCHANGE_BUFFER_METHOD_NAME = "void " + Profiler.class.getName() + ".exchangeBuffer(" + LocalBuffer.class.getName() + ") (Profiler.java:81) [profiler-runtime.jar]";
    public static final int EXCHANGE_BUFFER_METHOD_ID = ProfilerData.resolveTag(EXCHANGE_BUFFER_METHOD_NAME) | DumperConstants.DATA_ENTER_RECORD;

    public static void exchangeBuffer(LocalBuffer buffer) {
        final LocalState state = buffer.state;

        //if this is itself a dumpber thread or the dumper is dead
        if(state.isSystem || ProfilerData.dumperDead) {
            buffer.reset();
            return;
        }

        LocalBuffer newBuffer = ProfilerData.getEmptyBuffer(state);
        newBuffer.init(buffer);

        //if buffer is not corrupted, but state.isSystem=true, buffer is successfully put to dirty buffers. That's why we need to split the ifs in 2
        buffer.corrupted = !ProfilerData.addDirtyBuffer(buffer, ProfilerData.BLOCK_WHEN_DIRTY_BUFFERS_QUEUE_IS_FULL);
        if (buffer.corrupted) {
            buffer.reset();
            newBuffer.reset();

            ProfilerData.addEmptyBuffer(newBuffer);

            return;
        }

        int exchangeBegin = TimerCache.timer;

        state.buffer = newBuffer;

        final int exchangeEnd = TimerCache.timer;

        int exchangeLength = exchangeEnd - exchangeBegin;
        if (exchangeLength > 0) {
            newBuffer.data[0] = EXCHANGE_BUFFER_METHOD_ID | (((long) exchangeBegin) << 32);
            newBuffer.data[1] = (((long) exchangeEnd) << 32);
            newBuffer.count = 2;
        }
    }

    public static void exchangeBuffer(LocalBuffer buffer,
                                      long methodAndTime) {
        final LocalState state = buffer.state;
        exchangeBuffer(buffer);

        LocalBuffer newBuffer = state.buffer;
        long[] data = newBuffer.data;

        int count = newBuffer.count;
        data[count] = methodAndTime;

        newBuffer.count = count + 1;
    }

    public static MetricsConfiguration getMetricConfigByName(String callType) {
        final ProfilerTransformerPlugin transformer = Bootstrap.getPlugin(ProfilerTransformerPlugin.class);
        if (transformer == null) {
            logger.warning("ProfilerTransformerPlugin not loaded");
            return null;
        }

        final Configuration_03 conf = (Configuration_03) transformer.getConfiguration();
        MetricsConfiguration result = null;
        for (MetricsConfiguration metricsConfiguration : conf.getMetricsConfig()) {
            if (metricsConfiguration.getName().equals(callType)) {
                result = metricsConfiguration;
                break;
            }
        }

        return result;
    }

    public static Metric getMetric(MetricType metricType, String callType, Map<String, String> aggregationParameters) {
        MetricsPlugin metricsPlugin = Bootstrap.getPlugin(MetricsPlugin.class);
        if (metricsPlugin == null) {
            logger.warning("MetricsPlugin not loaded");
            return null;
        }

        return metricsPlugin.getMetric(metricType, callType, aggregationParameters);
    }
}
