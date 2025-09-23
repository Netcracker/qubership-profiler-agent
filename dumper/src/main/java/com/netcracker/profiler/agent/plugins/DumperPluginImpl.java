package com.netcracker.profiler.agent.plugins;

import static com.netcracker.profiler.Dumper.PROFILER_TITLE;
import static com.netcracker.profiler.agent.ProfilerData.DISABLE_CALL_EXPORT;

import com.netcracker.profiler.Dumper;
import com.netcracker.profiler.agent.*;
import com.netcracker.profiler.client.CollectorClientFactory;
import com.netcracker.profiler.dump.DumpRootResolver;
import com.netcracker.profiler.dump.DumperThread;
import com.netcracker.profiler.dump.ThreadState;
import com.netcracker.profiler.metrics.MetricsPluginImpl;
import com.netcracker.profiler.transfer.DataSender;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;

public class DumperPluginImpl implements DumperPlugin_10 {
    private final static Logger log = LoggerFactory.getLogger(DumperPluginImpl.class);
    Dumper dumper;
    DumperThread dumperThread;
    DataSender dataSender;
    private BlockingQueue<LocalBuffer> dirtyBuffers;
    private BlockingQueue<LocalBuffer> emptyBuffers;
    private ConcurrentMap<Thread, LocalState> activeThreads;
    private MetricsPluginImpl metricsPlugin;

    private final Semaphore startingSemaphore = new Semaphore(1);

    public DumperPluginImpl() {
        Bootstrap.registerPlugin(DumperPlugin.class, this);
        metricsPlugin = new MetricsPluginImpl();
    }

    public void newDumper(BlockingQueue<LocalBuffer> dirtyBuffers, BlockingQueue<LocalBuffer> emptyBuffers, ArrayList<LocalBuffer> buffers) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void newDumper(BlockingQueue<LocalBuffer> dirtyBuffers, BlockingQueue<LocalBuffer> emptyBuffers, ConcurrentMap<Thread, LocalState> activeThreads) {
        this.dirtyBuffers = dirtyBuffers;
        this.emptyBuffers = emptyBuffers;
        this.activeThreads = activeThreads;
        start();
    }

    public void reconfigure() {
        final ProfilerTransformerPlugin transformer = Bootstrap.getPlugin(ProfilerTransformerPlugin.class);
        final Configuration_05 conf = (Configuration_05) transformer.getConfiguration();

        conf.getParameterInfo(PROFILER_TITLE).index(true);

        dumper.configure(conf.getParametersInfo(), conf.getLogMaxAge(), conf.getLogMaxSize(), conf.getMetricsConfig(), conf.getSystemMetricsConfig());

        reconfigureCallsExporter(conf.getNetworkExportParams());
    }

    private void reconfigureCallsExporter(NetworkExportParams exportParams) {
        if(DISABLE_CALL_EXPORT) return;
        if(exportParams != null) {
            if(dataSender == null) {
                dataSender = new DataSender(exportParams);
                dataSender.start();
            } else {
                dataSender.configure(exportParams);
            }
            dumper.getDumperCallsExporter().configureExport(dataSender.getJsonsToSend(), dataSender.getEmptyJsonBuffers(), exportParams);
        } else if(dataSender != null) {
            dumper.getDumperCallsExporter().configureExport(null, null, null);
            dataSender.shutdown();
            dataSender = null;
        }
    }

    public File getCurrentRoot() {
        final Dumper dumper = this.dumper;
        if (dumper == null)
            return null;
        return dumper.getCurrentRoot();
    }

    public List<String> getTags() {
        return ProfilerData.getTags();
    }

    public boolean start() {
        if (!startingSemaphore.tryAcquire())
            throw new IllegalStateException("Start/shutdown is in progress. Please, try later");
        try {
            if (dumper != null || dumperThread != null)
                throw new IllegalStateException("Cannot start dumper as there is another running one");

            String dumpFolder = DumpRootResolver.dumpRoot;
            log.info("Using the following dump folder: {}", dumpFolder);

            dumper = new Dumper(dirtyBuffers, emptyBuffers, activeThreads, dumpFolder, metricsPlugin);
            reconfigure();
            dumperThread = new DumperThread(dumper, "Profiler results dumper " + dumpFolder);
            return false;
        } finally {
            startingSemaphore.release();
        }
    }

    public boolean stop(boolean force) {
        if (!startingSemaphore.tryAcquire())
            throw new IllegalStateException("Start/shutdown is in progress. Please, try later");
        try {
            if (dumperThread == null) return false;
            try {
                dumperThread.shutdown();
                if (force)
                    dumperThread.interrupt();
                dumperThread.join(5000);
                dumperThread = null;
                dumper = null;
                return true;
            } catch (InterruptedException e) {
                throw new IllegalStateException("Unable to shutdown dumper in 5 seconds", e);
            }
        } finally {
            startingSemaphore.release();
        }
    }

    public boolean isStarted() {
        return dumperThread != null && dumperThread.isAlive() && !ProfilerData.dumperDead;
    }

    public int getNumberOfRestarts() {
        return ProfilerData.dumperIncarnation;
    }

    public long getWrittenRecords() {
        final Dumper dumper = this.dumper;
        if (dumper == null)
            return 0;
        return dumper.getRecordsWritten();
    }

    public long getWrittenBytes() {
        final Dumper dumper = this.dumper;
        if (dumper == null)
            return 0;
        return dumper.getCompressedSize();
    }

    public long getUncompressedSize() {
        final Dumper dumper = this.dumper;
        if (dumper == null)
            return 0;
        return dumper.getUncompressedSize();
    }

    public long getWriteTime() {
        final Dumper dumper = this.dumper;
        if (dumper == null)
            return 0;
        return dumper.getDumpTime();
    }

    public long getDumperStartTime() {
        final Dumper dumper = this.dumper;
        if (dumper == null)
            return 0;
        return dumper.getDumperStartTime();
    }

    public Object[] getInflightCalls() {
        if (!isStarted()) return null;
        // Result is a pair of {File dumpRoot, List<Object[]> callData}
        Object[] result = new Object[2];
        callDumper(DumperConstants.COMMAND_GET_INFLIGHT_CALLS, result, 20000L);
        return result[0] != null ? result : null;
    }

    private boolean callDumper(byte commandId, Object[] arg, long timeout) {
        final LocalBuffer buffer = ProfilerData.getEmptyBuffer(Profiler.getState());

        int restarts = getNumberOfRestarts();

        buffer.command(commandId, arg);
        ProfilerData.addDirtyBuffer(buffer, true);

        long deadline = System.currentTimeMillis() + timeout;
        synchronized (arg) { // Dumper would send notify
            while (System.currentTimeMillis() < deadline && arg[0] == null && restarts == getNumberOfRestarts()) {
                try {
                    arg.wait(Math.max(deadline - System.currentTimeMillis(), 100));
                } catch (InterruptedException e) {
                    /* Ignore */
                }
            }
            return arg[0] != null;
        }
    }

    public long getBytesAllocated() {
        final Dumper dumper = this.dumper;
        if (dumper == null)
            return 0;
        final LocalState state = dumper.localState;
        if (state == null) return 0;
        ThreadJMXProviderFactory.INSTANCE.updateThreadCounters(state);
        ThreadState prevState = (ThreadState) state.additional;
        if (prevState == null) // in fact it should not be null
            return state.memoryUsed;
        return state.memoryUsed - prevState.prevMemoryUsed;
    }

    public long getCPUTime() {
        final Dumper dumper = this.dumper;
        if (dumper == null)
            return 0;
        final LocalState state = dumper.localState;
        if (state == null) return 0;
        ThreadJMXProviderFactory.INSTANCE.updateThreadCounters(state);
        ThreadState prevState = (ThreadState) state.additional;
        if (prevState == null) // in fact it should not be null
            return state.cpuTime;
        return state.cpuTime - prevState.prevCpuTime;
    }

    public long getFileRead() {
        final Dumper dumper = this.dumper;
        if (dumper == null)
            return 0;
        final LocalState state = dumper.localState;
        if (state == null) return 0;
        ThreadState prevState = (ThreadState) state.additional;
        if (prevState == null) // in fact it should not be null
            return state.fileRead;
        return state.fileRead - prevState.prevFileRead;
    }

    public long getFileWritten() {
        final Dumper dumper = this.dumper;
        if (dumper == null)
            return 0;
        final LocalState state = dumper.localState;
        if (state == null) return 0;
        ThreadState prevState = (ThreadState) state.additional;
        if (prevState == null) // in fact it should not be null
            return state.fileWritten;
        return state.fileWritten - prevState.prevFileWritten;
    }

    public boolean gracefulShutdown() {
        return gracefulShutdown(5000L);
    }

    public boolean gracefulShutdown(long timeout) {
        if (!isStarted()) return true;
        // No results
        Object[] result = new Object[1];
        return callDumper(DumperConstants.COMMAND_GRACEFUL_SHUTDOWN, result, timeout);
    }

    public long getArchiveSize() {
        final Dumper dumper = this.dumper;
        if (dumper == null)
            return 0;

        return dumper.getArchiveSize();
    }

    public void forceRescanDumpDir() {
        final Dumper dumper = this.dumper;
        if (dumper == null)
            return;

        dumper.forceRescanDumpDir();
    }

    public String getMetrics() {
        return metricsPlugin.getMetrics();
    }

    @Override
    public void injectCollectorClientFactory(DumperCollectorClientFactory toInject) {
        CollectorClientFactory.injectFactory(toInject);
    }

    @Override
    public DumperCollectorClientFactory getCollectorClientFactory() {
        return CollectorClientFactory.instance();
    }

    @Override
    public boolean isInitialized() {
        return dumper != null && dumper.isInitialized();
    }
}
