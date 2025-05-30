package org.qubership.profiler;

import static org.qubership.profiler.agent.PropertyFacadeBoot.getPropertyOrEnvVariable;

import org.qubership.profiler.agent.*;
import org.qubership.profiler.agent.DumperCollectorClient;
import org.qubership.profiler.client.CollectorClientFactory;
import org.qubership.profiler.cloud.transport.ProtocolConst;
import org.qubership.profiler.dump.DataOutputStreamEx;
import org.qubership.profiler.dump.DumpFileManager;
import org.qubership.profiler.dump.IDataOutputStreamEx;
import org.qubership.profiler.dump.ThreadState;
import org.qubership.profiler.formatters.title.ProfilerTitle;
import org.qubership.profiler.formatters.title.TitleFormatterFacade;
import org.qubership.profiler.io.InflightCallImpl;
import org.qubership.profiler.io.SuspendLog;
import org.qubership.profiler.io.listener.FileRotatedListener;
import org.qubership.profiler.metrics.MetricsPluginImpl;
import org.qubership.profiler.sax.builders.InMemorySuspendLogBuilder;
import org.qubership.profiler.sax.builders.InMemorySuspendLogBuilderStub;
import org.qubership.profiler.stream.CompressedLocalAndRemoteOutputStream;
import org.qubership.profiler.stream.ICompressedLocalAndRemoteOutputStream;
import org.qubership.profiler.util.DumperCallsExporter;
import org.qubership.profiler.util.MetricsCollector;
import org.qubership.profiler.util.MurmurHash;
import org.qubership.profiler.util.ThrowableHelper;
import org.qubership.profiler.util.cache.TLimitedLongLongHashMap;

import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.procedure.TIntObjectProcedure;
import gnu.trove.set.hash.THashSet;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

public class Dumper implements IDumper, org.qubership.profiler.agent.DumperConstants {
    private static final Logger log = LoggerFactory.getLogger(Dumper.class);
    ESCLogger escLogger = ESCLogger.getLogger(Dumper.class);
    private final static int BUFFER_STEAL_INTERVAL = Integer.getInteger(Dumper.class.getName() + ".BUFFER_STEAL_INTERVAL", 5);
    private final static int STREAM_FLUSH_INTERVAL = Integer.getInteger(Dumper.class.getName() + ".STREAM_FLUSH_INTERVAL", 5);
    private final static int BUFFER_SCALE_INTERVAL = Integer.getInteger(Dumper.class.getName() + ".BUFFER_SCALE_INTERVAL", 5);

    private final static int MAX_VALUES_PER_INDEXED_PARAM = Integer.getInteger(Dumper.class.getName() + ".MAX_VALUES_PER_INDEXED_PARAM", 100);

    public static final String DUMP_ITERATION_METHOD_NAME = "void " + Dumper.class.getName() + ".dumpIteration() (Dumper.java:200) [profiler-runtime.jar]";
    public static final int DUMP_ITERATION_METHOD_ID = org.qubership.profiler.agent.ProfilerData.resolveTag(DUMP_ITERATION_METHOD_NAME) | org.qubership.profiler.agent.DumperConstants.DATA_ENTER_RECORD;

    public static final String PROFILER_TITLE = "profiler.title";

    private DumperCallsExporter dumperCallsExporter;
    /*
    Streams:
      Trace
         Contain raw trace data
      Calls
         Contain top level call for each call with some arguments
      BigParams
         Contain large parameters
      BigParamsDedup
         Contain large parameters that might be reused (e.g. SQL, XPath)
     */
    File dumpRoot;

    ICompressedLocalAndRemoteOutputStream traceOs;
    ICompressedLocalAndRemoteOutputStream callsOs;
    ICompressedLocalAndRemoteOutputStream calls_100_500_Os;
    ICompressedLocalAndRemoteOutputStream calls_500_3s_Os;
    ICompressedLocalAndRemoteOutputStream calls_3s_60m_Os;
    ICompressedLocalAndRemoteOutputStream calls_60mPlus_Os;
    ICompressedLocalAndRemoteOutputStream bigParamsOs;
    ICompressedLocalAndRemoteOutputStream bigParamsDedupOs;
    ICompressedLocalAndRemoteOutputStream dictOs;
    ICompressedLocalAndRemoteOutputStream callsDictOs;
    ICompressedLocalAndRemoteOutputStream suspendOs;
    ICompressedLocalAndRemoteOutputStream gcOs;
    ICompressedLocalAndRemoteOutputStream paramInfoOs;
    List<ICompressedLocalAndRemoteOutputStream> outputStreams;
    List<ICompressedLocalAndRemoteOutputStream> remoteStreams;

    TLimitedLongLongHashMap dedupParamCache = new TLimitedLongLongHashMap(Integer.getInteger(Dumper.class.getName() + ".SQL_CACHE_SIZE", 10000));

    long lastSuspendLogEntry = TimerCache.startTime;
    long prevSuspendDate = -1;
    int prevSuspendDuration;

    int lastWrittenDictionaryTag;
    List<String> dictionary = org.qubership.profiler.agent.ProfilerData.getTags();

    Set<Integer> callsDictionaryIds = new HashSet<>();

    private final BlockingQueue<LocalBuffer> dirtyBuffers;
    private final BlockingQueue<LocalBuffer> emptyBuffers;
    private final ConcurrentMap<Thread, LocalState> buffers;

    private final String dumpRootFolder;
    private String relativeDumpRootPath;
    private TIntIntHashMap paramTypes = new TIntIntHashMap();
    private byte[] paramTypesStream;
    private boolean writeCallRanges;
    private boolean writeCallsDictionary;
    private static int PARAM_COMMON_STARTED = org.qubership.profiler.agent.ProfilerData.resolveTag("common.started");
    private static int PARAM_PROFILER_TITLE = org.qubership.profiler.agent.ProfilerData.resolveTag(PROFILER_TITLE);
    private static int PARAM_NODE_NAME = org.qubership.profiler.agent.ProfilerData.resolveTag("node.name");
    private static int PARAM_JAVA_THREAD = org.qubership.profiler.agent.ProfilerData.resolveTag("java.thread");
    private static int PARAM_LOG_GENERATED = org.qubership.profiler.agent.ProfilerData.resolveTag("log.generated");
    private static int PARAM_LOG_WRITTEN = org.qubership.profiler.agent.ProfilerData.resolveTag("log.written");
    private static int PARAM_CPU_TIME = org.qubership.profiler.agent.ProfilerData.resolveTag("time.cpu");
    private static int PARAM_WAIT_TIME = org.qubership.profiler.agent.ProfilerData.resolveTag("time.wait");
    private static int PARAM_MEMORY_ALLOCATED = org.qubership.profiler.agent.ProfilerData.resolveTag("memory.allocated");
    private static int PARAM_IO_DISK_READ = org.qubership.profiler.agent.ProfilerData.resolveTag("io.disk.read");
    private static int PARAM_IO_DISK_WRITTEN = org.qubership.profiler.agent.ProfilerData.resolveTag("io.disk.written");
    private static int PARAM_IO_NET_READ = org.qubership.profiler.agent.ProfilerData.resolveTag("io.net.read");
    private static int PARAM_IO_NET_WRITTEN = org.qubership.profiler.agent.ProfilerData.resolveTag("io.net.written");
    private static int PARAM_J2EE_TRANSACTIONS = org.qubership.profiler.agent.ProfilerData.resolveTag("j2ee.transactions");
    private static int PARAM_QUEUE_WAIT_TIME = org.qubership.profiler.agent.ProfilerData.resolveTag("time.queue.wait");

    public static final String PARAM_REMOTE_DUMP_HOST = "REMOTE_DUMP_HOST";
    public static final String PARAM_REMOTE_DUMP_PORT = "REMOTE_DUMP_PORT";
    public static final String PARAM_REMOTE_DUMP_PORT_PLAIN = "REMOTE_DUMP_PORT_PLAIN";
    public static final String PARAM_REMOTE_DUMP_PORT_SSL = "REMOTE_DUMP_PORT_SSL";
    public static final String PARAM_FORCE_LOCAL_DUMP = "FORCE_LOCAL_DUMP";
    public static final String PARAM_CLOUD_NAMESPACE = "CLOUD_NAMESPACE";
    public static final String PARAM_MICROSERVICE_NAME = "MICROSERVICE_NAME";

    int lastBufferStealTime;
    int lastStreamFlushTime;
    int lastBufferScaleTime;
    long recordsWritten = 0;
    int nextTimeWritePerformanceInfo = TimerCache.timer + 60 * 30;
    long nextIdleThreadWarningTime;
    long dumperStartTime = TimerCache.now;
    long dumpTime = 0;
    int methodsWritten = 0;
    private String dumpRootPath;
    private DumpFileManager dumpFileManager;
    private long logMaxAge, lastLogPurgeTimestamp;
    private long logMaxSize, lastLogPurgeSize;
    private long compressedBytesWrittenBaseline;
    private List<MetricsConfiguration> metricsConfiguration;
    public LocalState localState;
    private MetricsPluginImpl metricsPlugin;
    private InMemorySuspendLogBuilder inMemorySuspendLogBuilder;
    private DumperCollectorClient client;
    private boolean localDumpEnabled;
    private boolean remoteConfigured;
    private GCDumper gcDumper;
    private volatile boolean initialized = false;

    String cloudNamespace = getPropertyOrEnvVariable(PARAM_CLOUD_NAMESPACE);
    String microserviceName = getPropertyOrEnvVariable(PARAM_MICROSERVICE_NAME);
    String podName = ServerNameResolver.SERVER_NAME + "_" + System.currentTimeMillis();
    String remoteHost = getPropertyOrEnvVariable(PARAM_REMOTE_DUMP_HOST);
    String remotePortStringSSL = getPropertyOrEnvVariable(PARAM_REMOTE_DUMP_PORT_SSL);
    String remotePortStringPlain = getPropertyOrEnvVariable(PARAM_REMOTE_DUMP_PORT_PLAIN);
    String forceLocalDumpString = getPropertyOrEnvVariable(PARAM_FORCE_LOCAL_DUMP);

    public Dumper(BlockingQueue<LocalBuffer> dirtyBuffers,
                  BlockingQueue<LocalBuffer> emptyBuffers,
                  ConcurrentMap<Thread, LocalState> buffers,
                  String dumpFolder,
                  MetricsPluginImpl metricsPlugin) {
        this.dirtyBuffers = dirtyBuffers;
        this.emptyBuffers = emptyBuffers;
        this.buffers = buffers;
        dumpRootFolder = dumpFolder;
        this.metricsPlugin = metricsPlugin;
        dumperCallsExporter = new DumperCallsExporter();
        if(ProfilerData.INMEMORY_SUSPEND_LOG) {
            inMemorySuspendLogBuilder = new InMemorySuspendLogBuilder(ProfilerData.INMEMORY_SUSPEND_LOG_SIZE, ProfilerData.INMEMORY_SUSPEND_LOG_SIZE);
        } else {
            inMemorySuspendLogBuilder = new InMemorySuspendLogBuilderStub();
        }

        remoteConfigured = StringUtils.isNotEmpty(remoteHost);
        boolean forceLocalDump = StringUtils.isNotEmpty(forceLocalDumpString) && Boolean.parseBoolean(forceLocalDumpString);
        localDumpEnabled = forceLocalDump || !remoteConfigured;

        this.writeCallRanges = ProfilerData.WRITE_CALL_RANGES && localDumpEnabled; //WRITE_CALL_RANGES isn't supported for remote stream
        this.writeCallsDictionary = ProfilerData.WRITE_CALLS_DICTIONARY && localDumpEnabled; //WRITE_CALLS_DICTIONARY isn't supported for remote stream
        initStreams();

        log.info("Profiler dumper: Remote client connection parameters:\n{}:\t{}\n{}:\t{}\n{}:\t{}\n{}:\t{}\n{}:\t{}\n{}:\t{}\n{}:\t{}\n{}:\t{}\n{}:\t{}",
                PARAM_CLOUD_NAMESPACE, cloudNamespace,
                PARAM_MICROSERVICE_NAME, microserviceName,
                "pod name", podName,
                PARAM_REMOTE_DUMP_HOST, remoteHost,
                PARAM_REMOTE_DUMP_PORT_SSL, remotePortStringSSL,
                PARAM_REMOTE_DUMP_PORT_PLAIN, remotePortStringPlain,
                PARAM_FORCE_LOCAL_DUMP, forceLocalDumpString,
                "remote configured", remoteConfigured,
                "local dump enabled", localDumpEnabled
        );
    }

    private void initStreams() {
        this.traceOs = new CompressedLocalAndRemoteOutputStream("trace",
                Integer.getInteger(Dumper.class.getName() + ".TRACE_LOG_ROTATE_SIZE", 100 * 1024 * 1024), 0) {

            @Override
            public void fileRotated() throws IOException {
                getStream().writeLong(TimerCache.startTime);
            }

        };

        this.callsOs = new CallsCompressedLocalAndRemoteOutputStream("calls",
                Integer.getInteger(Dumper.class.getName() + ".CALLS_LOG_ROTATE_SIZE", 10 * 1024 * 1024), 4);

        this.calls_100_500_Os = writeCallRanges ? new CallsCompressedLocalAndRemoteOutputStream("calls[100ms-500ms]",
                Integer.getInteger(Dumper.class.getName() + ".CALLS_RANGE_LOG_ROTATE_SIZE", 1 * 1024 * 1024), 4) : null;

        this.calls_500_3s_Os = writeCallRanges ? new CallsCompressedLocalAndRemoteOutputStream("calls[500ms-3s]",
                Integer.getInteger(Dumper.class.getName() + ".CALLS_RANGE_LOG_ROTATE_SIZE", 1 * 1024 * 1024), 4) : null;

        this.calls_3s_60m_Os = writeCallRanges ? new CallsCompressedLocalAndRemoteOutputStream("calls[3s-60m]",
                Integer.getInteger(Dumper.class.getName() + ".CALLS_RANGE_LOG_ROTATE_SIZE", 1 * 1024 * 1024), 4) : null;

        this.calls_60mPlus_Os = writeCallRanges ? new CallsCompressedLocalAndRemoteOutputStream("calls[60m+]",
                Integer.getInteger(Dumper.class.getName() + ".CALLS_RANGE_LOG_ROTATE_SIZE", 1 * 1024 * 1024), 4) : null;
        this.bigParamsOs = new CompressedLocalAndRemoteOutputStream("xml",
                Integer.getInteger(Dumper.class.getName() + ".XML_LOG_ROTATE_SIZE", 100 * 1024 * 1024), 0
        );


        this.bigParamsDedupOs = new CompressedLocalAndRemoteOutputStream("sql",
                Integer.getInteger(Dumper.class.getName() + ".SQL_LOG_ROTATE_SIZE", 100 * 1024 * 1024), 0
        ) {
            @Override
            public void fileRotated() throws IOException {
                dedupParamCache.clear();
            }
        };
        bigParamsDedupOs.setDependentStream(traceOs);

        this.dictOs = new CompressedLocalAndRemoteOutputStream("dictionary", 0, 0 ){
            @Override
            protected boolean resetExistingContents() {
                return lastWrittenDictionaryTag == 0;
            }
        };
        this.callsDictOs = writeCallsDictionary ?
                new CompressedLocalAndRemoteOutputStream("callsDictionary", 0, 0 ) : null;
        this.suspendOs = new CompressedLocalAndRemoteOutputStream("suspend", 0, 0) {
            @Override
            public void fileRotated() throws IOException {
                getStream().writeLong(lastSuspendLogEntry);
            }
        };
        this.gcOs = new CompressedLocalAndRemoteOutputStream("gc",0, 0);

        this.paramInfoOs = new CompressedLocalAndRemoteOutputStream("params", 0, 0) {
            @Override
            public void fileRotated() throws IOException {
                getStream().write(paramTypesStream);
                writePhrase();
                getStream().flush();
                close();

            }

//        @Override
//        public CompressedLocalAndRemoteOutputStream rotate() throws IOException {
//            return getIndex() > 0 ? this : super.rotate();
//        }
        };

        this.outputStreams = new ArrayList<>(Arrays.asList(
                traceOs, callsOs, bigParamsOs, bigParamsDedupOs,
                dictOs, suspendOs, gcOs, paramInfoOs
        ));
        if(writeCallRanges) {
            outputStreams.addAll(Arrays.asList(calls_100_500_Os, calls_500_3s_Os, calls_3s_60m_Os, calls_60mPlus_Os));
        }
        if(writeCallsDictionary) {
            outputStreams.add(callsDictOs);
        }

        this.remoteStreams = Arrays.asList(
                traceOs, callsOs, bigParamsOs, bigParamsDedupOs,
                dictOs, suspendOs, gcOs, paramInfoOs
        );
    }

    private void initializeCollectorClient(){

        for (ICompressedLocalAndRemoteOutputStream stream : this.outputStreams) {
            stream.setLocalDumpEnabled(localDumpEnabled);
        }

        String remotePortString = remotePortStringSSL;
        boolean ssl = !StringUtils.isBlank(remotePortString);
        if(!ssl) {
            remotePortString = remotePortStringPlain;
        }

        if (remoteConfigured) {
            int remotePort = ProtocolConst.PLAIN_SOCKET_PORT;
            try {
                if(!StringUtils.isBlank(remotePortString)) {
                    remotePort = Integer.parseInt(remotePortString);
                }
            } catch (NumberFormatException e) {
                log.debug("Failed to parse remote dump port, use default port {}", remotePort);
            }

            this.client = CollectorClientFactory.instance().newClient(remoteHost, remotePort, ssl, cloudNamespace, microserviceName, podName);
            for (ICompressedLocalAndRemoteOutputStream stream : this.remoteStreams) {
                stream.setClient(client);
            }
        }
    }

    public void configure(Map<String, ParameterInfo> paramInfo, long logMaxAge, long logMaxSize, List<MetricsConfiguration> metrics, List<MetricsDescription> systemMetrics) {
        // Schedule next purge in 10 minutes
        if (this.logMaxAge != logMaxAge || this.logMaxSize != logMaxSize)
            lastLogPurgeTimestamp = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10) - logMaxAge * 2;

        this.logMaxAge = logMaxAge;
        this.logMaxSize = logMaxSize;
        this.metricsConfiguration = metrics;

        if (paramInfo == null) return;
        final TIntIntHashMap paramTypes = new TIntIntHashMap();
        List<ParameterInfo> listParams = new ArrayList<>();

        for (ParameterInfo info : paramInfo.values()) {
            paramTypes.put(org.qubership.profiler.agent.ProfilerData.resolveTag(info.name), info.combined);
            if(info.list) {
                listParams.add(info);
            }
        }
        this.paramTypes = paramTypes;
        paramTypesStream = prepareParamInfoStream(paramInfo);
        TitleFormatterFacade.setDefaultListParams(listParams);

        metricsPlugin.resetMetrics();
        MetricsCollector.resetCaches();
        metricsPlugin.createSystemMetrics(systemMetrics);
    }

    private byte[] prepareParamInfoStream(Map<String, ParameterInfo> paramInfo) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(paramInfo.size() * 40);
        DataOutputStreamEx out = new DataOutputStreamEx(baos);
        try {
            out.write(1); // format version
            for (ParameterInfo info : paramInfo.values()) {
                out.write(info.name);
                out.write(info.index ? 1 : 0);
                out.write(info.list ? 1 : 0);
                out.writeVarInt(info.order);
                out.write(info.signatureFunction == null ? "" : info.signatureFunction);
            }
            return baos.toByteArray();
        } catch (IOException e) {
            log.warn("Unable to write parameter types", e);
        } finally {
            close(out);
        }
        return null;
    }

    public boolean isInitialized(){
        return initialized;
    }

    public synchronized void close() throws IOException{
        this.initialized = false;

        for (int i = 0, outputStreamsSize = outputStreams.size(); i < outputStreamsSize; i++) {
            ICompressedLocalAndRemoteOutputStream stream = outputStreams.get(i);
            close(stream);
        }
        if (dumpFileManager != null) {
            dumpFileManager.close();
            dumpFileManager = null;
        }
        if(gcDumper != null) {
            gcDumper.close();
            gcDumper = null;
        }
        log.debug("Closed ESC dump files");
        if (client != null) {
            try {
                client.close();
                client = null;
                log.debug("RPC client has been shut down");
            } catch (IOException e) {
                log.error("Failed to wait till graceful shutdown of remote RPC connection, skip it.", e);
            }
        }
    }

    public void initialize() throws IOException {
        org.qubership.profiler.agent.Profiler.markSystem();
        dedupParamCache.clear();
        org.qubership.profiler.agent.ProfilerData.clearThreadsInfo();

        lastWrittenDictionaryTag = 0;
        lastBufferStealTime = TimerCache.timer;
        lastBufferScaleTime = TimerCache.timer;
        lastStreamFlushTime = TimerCache.timer;
        nextTimeWritePerformanceInfo = TimerCache.timer + 60 * 30;
        dumperStartTime = TimerCache.now;
        dumpTime = 0;
        recordsWritten = 0;

        final LocalState state = Profiler.getState();
        ThreadJMXProviderFactory.INSTANCE.updateThreadCounters(state);
        ThreadState ts = new ThreadState();
        state.additional = ts;
        ts.saveThreadCounters(state.callInfo);
        localState = state;

        final Date date = new Date();
        dumpRootPath = dumpRootFolder + File.separatorChar + new SimpleDateFormat("yyyy'" + File.separatorChar + "'MM'" + File.separatorChar + "'dd").format(date) + File.separatorChar + date.getTime();
        dumpRoot = new File(dumpRootPath);
        relativeDumpRootPath = calculateRelativeDumpRootFolder(dumpRootPath);

        long compressedBytes = 0;

        if (dumpFileManager != null) {
            dumpFileManager.close();
        }
        dumpFileManager = new DumpFileManager(this.logMaxAge, this.logMaxSize, dumpRootFolder);

        final DumpFileManager fm = dumpFileManager;
        FileRotatedListener listener = fm.getFileRotatedListener();

        initializeCollectorClient();
        gcDumper = new GCDumper(gcOs);

        for (int i = 0, outputStreamsSize = outputStreams.size(); i < outputStreamsSize; i++) {
            ICompressedLocalAndRemoteOutputStream stream = outputStreams.get(i);
            stream.setRoot(dumpRoot);
            // For now DumpFileManager.getFileRotatedListener() is the only listener. Clear all earlier set listeners
            stream.clearListeners();
            stream.addListener(listener);
            stream.rotate();
            compressedBytes += stream.getCompressedSize();
        }

        compressedBytesWrittenBaseline = compressedBytes;

        this.initialized = true;
    }

    private String calculateRelativeDumpRootFolder(String dumpRootPath) { //IN: /u02/qubership/instance/execution-statistics-collector/dump/clust1_1989/2020/11/26/1606401808022
        Path p = Paths.get(dumpRootPath);
        return p.subpath(p.getNameCount()-5, p.getNameCount()).toString(); //OUT: clust1_1989/2020/11/26/1606401808022
    }

    public void addEmptyBuffer(LocalBuffer buffer) {
        emptyBuffers.offer(buffer);
    }

    public void dumpLoop() throws InterruptedException, IOException {
        log.info("Profiler dumper initialized successfully. Data will be collected at least every {} sec. Data will be sent at least every {} sec",
                BUFFER_STEAL_INTERVAL,
                STREAM_FLUSH_INTERVAL
        );

        ArrayList<LocalBuffer> buffers = new ArrayList<LocalBuffer>();
        while (dirtyBuffers != null) { // JMockit coverage does not support while(true)
            buffers.clear();

            escLogger.printDirtyBufferWarningInCaseOfOverflow();
            if (dirtyBuffers.drainTo(buffers, 100) == 0) {
                final LocalBuffer firstBuffer = dirtyBuffers.poll(BUFFER_STEAL_INTERVAL, TimeUnit.SECONDS);
                if (firstBuffer != null) // e.g. queue is empty
                    buffers.add(firstBuffer);
            }

            // Explicit enter and exit are here to ensure Dumper would not emit lots of calls due to usage of FileOutputStream.write
            // Lots of calls produce lots of CallInfo objects, consuming up to 60Mb in Dumper's LocalState
            localState.enter(DUMP_ITERATION_METHOD_ID);
            try {
                int dirtyLength = buffers.size();
                long t0 = System.nanoTime();
                for (int i = 0; i < dirtyLength; i++) {
                    LocalBuffer buffer = buffers.get(i);
                    if (buffer.corrupted) {
                        log.error("Corrupted buffer is in dirtyBuffers queue {}", buffer);
                        continue;
                    }
                    if (buffer.state == null) {
                        log.error("Buffer {} (prevBuffer=={}) with null state is found in dirtyBuffers queue.", buffer, buffer.prevBuffer);
                        continue;
                    }
                    if (buffer.count == -1) {
                        /* Not a real buffer, but command */
                        switch ((int) buffer.data[0]) {
                            case COMMAND_ROTATE_LOG:
                                rotateDumpFile();
                                break;
                            case COMMAND_FLUSH_LOG:
                                flushDumpFile();
                                break;
                            case COMMAND_EXIT:
                                close();
                                return;
                            case COMMAND_GRACEFUL_SHUTDOWN:
                                stealDataFromBuffers();
                                if (TimerCache.lastLoggedEvent != TimerCache.lastSuspendEvent) {
                                    dumpSuspendLog();
                                }
                                if (lastWrittenDictionaryTag < dictionary.size()) {
                                    dumpDictionary();
                                }
                                gcDumper.dumpGC();
                                flushDumpFile();
                                close();
                                Object result = buffer.value[0];
                                synchronized (result) {
                                    Object[] res = (Object[]) result;
                                    res[0] = "DONE";
                                    result.notify();
                                }
                                buffer.value[0] = null;
                                //since all dumping operations are duplicated here anyway
                                //but if we do not return and try to do the dumping operations on a closed dumper, we're going to get errors
                                return;
                            case COMMAND_GET_INFLIGHT_CALLS:
                                flushDumpFile();
                                collectInflightCalls(buffer.value[0]);
                                buffer.value[0] = null;
                        }
                        cleanupBuffer(buffer);
                        buffer.count = 0;
                        addEmptyBuffer(buffer);
                        continue;
                    }
                    if (buffer.count > buffer.first) {
                        recordsWritten += writeBuffer(buffer);
                    }
                    cleanupBuffer(buffer);
                    addEmptyBuffer(buffer);
                }

                if (TimerCache.lastLoggedEvent != TimerCache.lastSuspendEvent) {
                    dumpSuspendLog();
                }
                gcDumper.dumpGC();

                long t1 = System.nanoTime();
                long t2 = System.nanoTime();
                dumpTime += (t1 - t0) - (t2 - t1);

                boolean fileRotated = false;

                boolean isTraceRotated = false;
                boolean isCallsRotated = false;

                for (int i = 0, outputStreamsSize = outputStreams.size(); i < outputStreamsSize; i++) {
                    ICompressedLocalAndRemoteOutputStream stream = outputStreams.get(i);

                    boolean rotated = stream.rotateIfRequired();


                    if ("trace".equals(stream.getName())) {
                        isTraceRotated = rotated;
                    }

                    if ("calls".equals(stream.getName())) {
                        isCallsRotated = rotated;
                    }

                    fileRotated |= rotated;
                }

                if (recordsWritten > 0 && nextTimeWritePerformanceInfo - TimerCache.timer < 0) {
                    long compressedSize = getCompressedSize();
                    long uncompressedSize = getUncompressedSize();
                    log.debug("Processed {} records, average rate is rate = {} ns/record, written {} MiB total" +
                                    " (uncompressed size is {} MiB, compression rate {})," +
                                    " {} MiB is written since last restart of dumper ." +
                                    " Written {} bytes since last purge (will purge when reach {} bytes or {})"
                            , new Object[]{recordsWritten, dumpTime / (recordsWritten + 0.001), compressedSize / 1024 / 1024
                                    , uncompressedSize / 1024 / 1024, uncompressedSize / (compressedSize + 0.001)
                                    , (compressedSize - compressedBytesWrittenBaseline) / 1024 / 1024
                                    , compressedSize - lastLogPurgeSize, logMaxSize * 2, new Date(lastLogPurgeTimestamp + logMaxAge * 2)});
                    nextTimeWritePerformanceInfo = TimerCache.timer + 30 * 1000;
                }

                if (TimerCache.timer - lastBufferStealTime > TimeUnit.SECONDS.toMillis(BUFFER_STEAL_INTERVAL)) {
                    stealDataFromBuffers();
                }

                if (TimerCache.timer - lastBufferScaleTime > TimeUnit.SECONDS.toMillis(BUFFER_SCALE_INTERVAL)) {
                    scaleBuffers();
                }

                if (lastWrittenDictionaryTag < dictionary.size()) {
                    dumpDictionary();
                }

                if (TimerCache.timer - lastStreamFlushTime > TimeUnit.SECONDS.toMillis(STREAM_FLUSH_INTERVAL)) {
                    flushDumpFile();
                }
            } finally {
                localState.exit();
            }
        }
    }

    private static final List<String> LIST_WITH_EMPTY_STRING = Collections.singletonList("");

    private void collectInflightCalls(final Object result) {
        ArrayList<InflightCall> calls = new ArrayList<InflightCall>();

        long nowLong = TimerCache.now;
        final int now = (int) (nowLong - TimerCache.startTime);
        for (LocalState state : buffers.values()) {
            if (!(state.additional instanceof ThreadState))
                continue;

            ThreadState ts = (ThreadState) state.additional;
            if (ts.method == 0) continue;
            final CallInfo callInfo = ts.callInfo;
            if (callInfo == null) {
                log.info("Detected active thread {}, however unable to identify what it is doing, since callInfo is null. Thread state: {}", state.shortThreadName, ts);
                continue;
            }

            InflightCallImpl call = new InflightCallImpl();
            call.time = nowLong - (now - ts.time);
            call.method = ts.method;
            call.calls = ts.calls;
            call.traceFileIndex = ts.traceFileIndex;
            call.bufferOffset = ts.bufferOffset;
            call.recordIndex = ts.recordIndex;
            call.threadName = state.thread.getName();
            call.queueWaitDuration = callInfo.queueWaitDuration;
            call.logsGenerated = callInfo.logGenerated;
            call.logsWritten = callInfo.logWritten;
            final Map<Integer, List<String>> params = call.params = new HashMap<Integer, List<String>>(ts.params.size() + 1, 1);
            params.put(TAGS_CALL_ACTIVE, LIST_WITH_EMPTY_STRING);
            if (callInfo.next == null) { // the call did not yet complete
                call.duration = now - ts.time;
                ThreadJMXProviderFactory.INSTANCE.updateThreadCounters(state);
                call.cpuTime = state.cpuTime - ts.prevCpuTime;
                call.waitTime = state.waitTime - ts.prevWaitTime;
                call.memoryUsed = state.memoryUsed - ts.prevMemoryUsed;
                call.fileRead = state.fileRead - ts.prevFileRead;
                call.fileWritten = state.fileWritten - ts.prevFileWritten;
                call.netRead = state.netRead - ts.prevNetRead;
                call.netWritten = state.netWritten - ts.prevNetWritten;
                call.transactions = state.transactions - ts.prevTransactions;
            } else { // the call has just finished
                call.duration = callInfo.finishTime - ts.time;
                call.cpuTime = callInfo.cpuTime - ts.prevCpuTime;
                call.waitTime = callInfo.waitTime - ts.prevWaitTime;
                call.memoryUsed = callInfo.memoryUsed - ts.prevMemoryUsed;
                call.fileRead = callInfo.fileRead - ts.prevFileRead;
                call.fileWritten = callInfo.fileWritten - ts.prevFileWritten;
                call.netRead = callInfo.netRead - ts.prevNetRead;
                call.netWritten = callInfo.netWritten - ts.prevNetWritten;
                call.transactions = callInfo.transactions - ts.prevTransactions;
            }
            if (!ts.params.isEmpty()) {
                ts.params.forEachEntry(new TIntObjectProcedure<THashSet<String>>() {
                    public boolean execute(int i, THashSet<String> strings) {
                        params.put(i, Arrays.asList(strings.toArray(new String[strings.size()])));
                        return true;
                    }
                });
                ProfilerTitle title = TitleFormatterFacade.formatTitle(call.method, ts.params);
                params.put(PARAM_PROFILER_TITLE, Collections.singletonList(title.getHtml()));
            }
            calls.add(call);
        }

        synchronized (result) {
            Object[] res = (Object[]) result;
            res[0] = dumpRoot;
            res[1] = calls;
            result.notify();
        }
    }

    private void scaleBuffers() {
        int addedCount = 0;
        for (int i = 0; i < ProfilerData.MAX_SCALE_ATTEMPTS; i++) {
            int emptyBuffers = this.emptyBuffers.size();
            int dirtyBuffers = this.dirtyBuffers.size();
            int activeThreads = buffers.size();
            if (emptyBuffers + dirtyBuffers >= Math.max(ProfilerData.MIN_BUFFERS, activeThreads)) {
                break;
            }
            boolean added = this.emptyBuffers.offer(new LocalBuffer());
            if (added) {
                addedCount++;
            } else {
                log.debug("Unable to add new empty buffer. emptyBuffers.size(): {}, dirtyBuffers.size(): {}, buffers.size(): {}, ProfilerData.MIN_BUFFERS: {}, addedCount: {}"
                        , new Object[]{emptyBuffers, dirtyBuffers, activeThreads, ProfilerData.MIN_BUFFERS, addedCount}
                );
                return;
            }
        }
        if (addedCount > 0) {
            log.debug("Added {} buffers for ensuring (empty+dirty).size >= MIN_BUFFERS. emptyBuffers.size(): {}, dirtyBuffers.size(): {}, buffers.size(): {}, ProfilerData.MIN_BUFFERS: {}"
                    , new Object[]{addedCount, emptyBuffers.size(), dirtyBuffers.size(), buffers.size(), ProfilerData.MIN_BUFFERS}
            );
        }
        lastBufferScaleTime = TimerCache.timer;
    }

    private void stealDataFromBuffers() throws IOException, InterruptedException {
        final long latestTimeToSteal = TimerCache.now - TimeUnit.SECONDS.toMillis(BUFFER_STEAL_INTERVAL);
        final long nextWarningTime = TimerCache.now + TimeUnit.SECONDS.toMillis(3600);
        boolean idleThreadsDetected = false;
        for (LocalState state : buffers.values()) {
            LocalBuffer buffer = state.buffer;
            if (buffer.corrupted) {
                continue;
            }
            if (buffer == null || buffer.count == -1) continue;
            if (buffer.startTime > latestTimeToSteal) continue;
            if (buffer.prevBuffer != null && buffer.prevBuffer.state == state) continue;
            if (state.isSystem) continue;
            boolean threadIsAlive = state.thread.isAlive();
            if (!threadIsAlive) {
                if (buffers.remove(state.thread) == null) {
                    log.debug("Thread {} info was already collected", state.thread.getName());
                    continue;
                }
                log.info("Detected dead thread {} during buffer steal", state.thread.getName());
                writeBuffer(buffer);
                //do not attempt to recover buffers from dying threads. concurrency issues may occur
//                cleanupBuffer(buffer);
//                addEmptyBuffer(buffer);
                continue;
            }

            if (buffer.count == buffer.first) {
                if (nextIdleThreadWarningTime < buffer.startTime) {
                    log.trace("Detected thread {} that is still alive, while it did not produce any profiled events since {}", state.thread.getName(), new Date(buffer.startTime));
                    idleThreadsDetected = true;
                }
                continue;
            }

            writeBuffer(buffer);
        }
        lastBufferStealTime = TimerCache.timer;
        if (idleThreadsDetected)
            nextIdleThreadWarningTime = nextWarningTime;
    }

    private int writeBuffer(LocalBuffer buffer) throws IOException {
//        if (client != null) {
//            log.trace("Write trace buffer started at {} to remote client", buffer.startTime);
//            client.writeTrace(buffer);
//        }
        log.trace("Write trace buffer started at {}", buffer.startTime);
        return writeBufferToFS(buffer);
    }

    private int writeBufferToFS(LocalBuffer buffer) throws IOException {
        final LocalState state = buffer.state;
        Object additional = state.additional;
        ThreadState thread = null;
        if (additional instanceof ThreadState)
            thread = (ThreadState) additional;

        final int last = buffer.count;
        int offs = buffer.first;

        if (thread == null) {
            CallInfo callInfo = null;
            // The very first buffer in thread has prevBuffer == null
            // Even after Dumper crash, we dump those buffers as is
            if (state.dumperIncarnation != org.qubership.profiler.agent.ProfilerData.dumperIncarnation) {
                // Non null prevBuffer in unknownThread happens in crash recovery
                // This means we must ignore all the records in buffer
                // until we find CallInfo record.
                final long[] data = buffer.data;
                int i;
                for (i = offs; i < last; i++) {
                    int typeAndId = (int) data[i];
                    if (typeAndId == ProfilerData.PARAM_CALL_INFO) {
                        callInfo = (CallInfo) buffer.value[i];
                        break;
                    }
                }
                if (callInfo == null) {
                    // We did not find callInfo record, thus returning
                    long startOffset = data[last - 1] >>> 32;
                    startOffset -= data[buffer.first] >>> 32;
                    buffer.first = i;
                    buffer.startTime += startOffset;
                    return 0;
                }
                // We found callInfo record and start dumping from the next record
                offs = i + 1;
                buffer.first = offs;
            }

            thread = new ThreadState();
            if (callInfo != null)
                thread.saveThreadCounters(callInfo);
            else {
                callInfo = state.callInfo;
                if (callInfo.isFirstInThread)
                    thread.callInfo = callInfo;
            }
            state.additional = thread;
        }

        int count = last - offs;

        if (count == 0) return 0;

        int prevMillis = 0;

        final IDataOutputStreamEx traceOs = this.traceOs.getStream();

        final int bufferOffset = traceOs.size();

        traceOs.writeLong(buffer.state.thread.getId());
        traceOs.writeLong(buffer.startTime);
        final long[] data = buffer.data;

        final Object[] values = buffer.value;

        for (int i = offs; i < last; i++) {
            long item = data[i];
            int typeAndId = (int) item;
            int type = typeAndId >>> 24;

            if (type == DATA_TAG_FIELD && values[i] == null) {
                offs++; /* so the recordIndex would account this skipped item */
                continue;
            }

            final int curMillis = (int) (item >>> 32);
            int millis = curMillis - prevMillis;
            prevMillis = curMillis;

            int b = ((type ^ 1) | ((millis & 0x1f) << 2));
            if (millis <= 0x1f && millis >= 0) {
                traceOs.write(b);
            } else {
                traceOs.write(b | 0x80);
                traceOs.writeVarInt(millis >> 5);
            }

            if (typeAndId == DATA_EXIT_RECORD)
                continue;

            int id = typeAndId & DATA_ID_MASK;

            if (type == DATA_ENTER_FIELD) {
                traceOs.writeVarInt(id);

                if (thread.method == 0) {
                    thread.time = curMillis;
                    thread.calls = 0;
                    thread.method = id;
                    thread.traceFileIndex = this.traceOs.getIndex();
                    thread.bufferOffset = bufferOffset;
                    thread.recordIndex = i - offs;
                }
                thread.calls++;
                continue;
            }
            final Object o = values[i];
            values[i] = null;
            if (o == null)
                throw new ProfilerAgentException("Looks like there is a corruption in buffer contents for buffer " + buffer + ", index " + i + ", thread " + state.thread);

            String value;
            if (o instanceof String) {
                value = (String) o;
            } else if (o instanceof StringBuffer || o instanceof StringBuilder || o instanceof Number) {
                value = o.toString();
            } else if (o instanceof Throwable) {
                value = ThrowableHelper.throwableToString((Throwable) o);
            } else if (o instanceof CallInfo) {
                CallInfo callInfo = (CallInfo) o;
                ProfilerTitle profilerTitle = TitleFormatterFacade.formatTitle(thread.method, thread.params);
                final long callDuration = curMillis - thread.time + callInfo.additionalReportedTime;
                MetricsCollector.collectMetrics(metricsPlugin, thread, metricsConfiguration, callDuration, callInfo, thread, buffer.state.thread.getName());
                writeParam(thread, id, "");

                if (thread.calls > 1 ||
                        callDuration > 20 ||
                        callInfo.isPersist > 0) {
                    long startTimestamp = buffer.startTime + (int) (thread.time - (int) (buffer.startTime - TimerCache.startTime));
                    if(dumperCallsExporter.isEnabled()) {
                        int suspension = getSuspension(startTimestamp, startTimestamp+callDuration);
                        dumperCallsExporter.exportCall(startTimestamp, callDuration, suspension, callInfo, profilerTitle, thread, buffer.state.thread.getName(), relativeDumpRootPath);
                    }
                    traceOs.write(EVENT_TAG_RECORD);
                    writeParam(thread, PARAM_COMMON_STARTED, Long.toString(startTimestamp));
                    offs--;

                    if(!profilerTitle.isDefault()) {
                        traceOs.write(EVENT_TAG_RECORD);
                        writeParam(thread, PARAM_PROFILER_TITLE, profilerTitle.getHtml());
                        offs--;
                    }
                    traceOs.write(EVENT_TAG_RECORD);
                    writeParam(thread, PARAM_NODE_NAME, ServerNameResolver.SERVER_NAME);
                    offs--;
                    traceOs.write(EVENT_TAG_RECORD);
                    writeParam(thread, PARAM_JAVA_THREAD, buffer.state.thread.getName());
                    offs--;
                    offs = writeCallParams(traceOs, thread, callInfo, offs);

                    writeCall(callInfo, thread, callDuration, state.thread);
                }
                // buffer does not contain this exit record as callInfo is always the last event in call
                // thus we explicitly write this event_exit_record
                traceOs.write(EVENT_EXIT_RECORD);
                offs--; // Ensure correct rowid is used as we just inserted non-existent-in-buffer exit record
                thread.params.clear();
                thread.method = 0;
                thread.saveThreadCounters(callInfo);
                callInfo.clean(); // stop nepotism
                continue;
            } else if (o instanceof org.qubership.profiler.agent.BigValueHolder) {
                org.qubership.profiler.agent.BigValueHolder h = (org.qubership.profiler.agent.BigValueHolder) o;
                Object val = h.getValue();
                if (h.getIndex() != -1) {
                    // We see same param the second time, thus just log reference (index,offset)
                    traceOs.writeVarInt(id);
                    final int paramType = paramTypes.get(id);
                    traceOs.write(paramType); // must be ParamTypes.PARAM_BIG or ParamTypes.PARAM_BIG_DEDUP
                    traceOs.writeVarInt(h.getIndex());
                    traceOs.writeVarInt(h.getOffset());
                    continue;
                }
                // The first time the parameter is logged. Will log value and get reference later
                value = val.toString();
            } else {
                value = "Object " + o.toString();
            }
            writeParam(thread, id, value, o instanceof org.qubership.profiler.agent.BigValueHolder ? (org.qubership.profiler.agent.BigValueHolder) o : null);
            value = null;
        }
        traceOs.write(EVENT_FINISH_RECORD);
        long startOffset = data[last - 1] >>> 32;
        startOffset -= data[buffer.first] >>> 32;
        buffer.startTime += startOffset;
        buffer.first = last;
        return count;
    }

    private int getSuspension(long begin, long end) {
        SuspendLog suspendLog = inMemorySuspendLogBuilder.get();
        return suspendLog.getSuspendDuration(begin, end);
    }

    private void writeCall(CallInfo callInfo, ThreadState threadState, long callDuration, Thread thread) throws IOException {
        writeCall(callInfo, threadState, callDuration, thread, this.callsOs);
        if(!writeCallRanges) {
            return;
        }
        if(callDuration < 100) {
            //DoNothing
        } else if(callDuration < 500) {
            writeCall(callInfo, threadState, callDuration, thread, this.calls_100_500_Os);
        } else if(callDuration < 3000) {
            writeCall(callInfo, threadState, callDuration, thread, this.calls_500_3s_Os);
        } else if(callDuration < 60 * 60 * 1000) {
            writeCall(callInfo, threadState, callDuration, thread, this.calls_3s_60m_Os);
        } else {
            writeCall(callInfo, threadState, callDuration, thread, this.calls_60mPlus_Os);
        }
    }

    private void writeCall(CallInfo callInfo, ThreadState threadState, long callDuration, Thread thread, ICompressedLocalAndRemoteOutputStream callsStream) throws IOException {
        IDataOutputStreamEx callsOs = callsStream.getStream();
        CallsState callsState = (CallsState) callsStream.getState();
        callsOs.writeVarIntZigZag(threadState.time - callsState.callsTimer);
        callsState.callsTimer = threadState.time;
        callsOs.writeVarInt(threadState.method);
        writeCallsDictionary(threadState.method);
        callsOs.writeVarInt(callDuration);
        callsOs.writeVarInt(threadState.calls);
        int threadIndex = callsState.threadIdsCache.get(thread.getId());
        if (threadIndex >= 0)
            callsOs.writeVarInt(threadIndex);
        else {
            threadIndex = callsState.threadIdsCounter;
            callsState.threadIdsCounter = threadIndex + 1;
            callsState.threadIdsCache.put(thread.getId(), threadIndex);
            callsOs.writeVarInt(threadIndex);
            callsOs.write(thread.getName());
        }
        callsOs.writeVarInt(callInfo.logWritten);
        callsOs.writeVarInt(callInfo.logGenerated - callInfo.logWritten);
        callsOs.writeVarInt(threadState.traceFileIndex);
        callsOs.writeVarInt(threadState.bufferOffset);
        callsOs.writeVarInt(threadState.recordIndex);
        callsOs.writeVarInt((int) (callInfo.cpuTime - threadState.prevCpuTime));
        callsOs.writeVarInt((int) (callInfo.waitTime - threadState.prevWaitTime));
        callsOs.writeVarInt(callInfo.memoryUsed - threadState.prevMemoryUsed);
        callsOs.writeVarInt(callInfo.fileRead - threadState.prevFileRead);
        callsOs.writeVarInt(callInfo.fileWritten - threadState.prevFileWritten);
        callsOs.writeVarInt(callInfo.netRead - threadState.prevNetRead);
        callsOs.writeVarInt(callInfo.netWritten - threadState.prevNetWritten);
        callsOs.writeVarInt(callInfo.transactions - threadState.prevTransactions);
        callsOs.writeVarInt(callInfo.queueWaitDuration);

        callsOs.writeVarInt(threadState.params.size());
        TIntObjectIterator<THashSet<String>> iterator = threadState.params.iterator();
        while (iterator.hasNext()) {
            iterator.advance();
            int id = iterator.key();
            THashSet<String> set = iterator.value();

            callsOs.writeVarInt(id);
            writeCallsDictionary(id);
            final int size = set.size();
            callsOs.writeVarInt(size);
            for(String value : set) {
                callsOs.write(value);
            }
        }
        // TODO: write call info values
    }

    private void writeCallsDictionary(int idx) throws IOException {
        if(!writeCallsDictionary) {
            return;
        }
        if(!callsDictionaryIds.contains(idx)) {
            callsDictionaryIds.add(idx);
            IDataOutputStreamEx stream = callsDictOs.getStream();
            stream.writeVarInt(idx);
            stream.write(dictionary.get(idx));
        }
    }

    private int writeCallParams(IDataOutputStreamEx traceOs, ThreadState thread, CallInfo callInfo, int offs) throws IOException {
        if (callInfo.logGenerated > 0) {
            traceOs.write(EVENT_TAG_RECORD);
            writeParam(thread, PARAM_LOG_GENERATED, Integer.toString(callInfo.logGenerated));
            offs--;
            if (callInfo.logWritten > 0) {
                traceOs.write(EVENT_TAG_RECORD);
                writeParam(thread, PARAM_LOG_WRITTEN, Integer.toString(callInfo.logWritten));
                offs--;
            }
        }
        long tmp;
        if ((tmp = callInfo.cpuTime - thread.prevCpuTime) != 0) {
            traceOs.write(EVENT_TAG_RECORD);
            writeParam(thread, PARAM_CPU_TIME, Long.toString(tmp));
            offs--;
        }
        if ((tmp = callInfo.waitTime - thread.prevWaitTime) != 0) {
            traceOs.write(EVENT_TAG_RECORD);
            writeParam(thread, PARAM_WAIT_TIME, Long.toString(tmp));
            offs--;
        }
        if ((tmp = callInfo.memoryUsed - thread.prevMemoryUsed) != 0) {
            traceOs.write(EVENT_TAG_RECORD);
            writeParam(thread, PARAM_MEMORY_ALLOCATED, Long.toString(tmp));
            offs--;
        }
        if ((tmp = callInfo.fileRead - thread.prevFileRead) != 0) {
            traceOs.write(EVENT_TAG_RECORD);
            writeParam(thread, PARAM_IO_DISK_READ, Long.toString(tmp));
            offs--;
        }
        if ((tmp = callInfo.fileWritten - thread.prevFileWritten) != 0) {
            traceOs.write(EVENT_TAG_RECORD);
            writeParam(thread, PARAM_IO_DISK_WRITTEN, Long.toString(tmp));
            offs--;
        }
        if ((tmp = callInfo.netRead - thread.prevNetRead) != 0) {
            traceOs.write(EVENT_TAG_RECORD);
            writeParam(thread, PARAM_IO_NET_READ, Long.toString(tmp));
            offs--;
        }
        if ((tmp = callInfo.netWritten - thread.prevNetWritten) != 0) {
            traceOs.write(EVENT_TAG_RECORD);
            writeParam(thread, PARAM_IO_NET_WRITTEN, Long.toString(tmp));
            offs--;
        }
        if ((tmp = callInfo.transactions - thread.prevTransactions) != 0) {
            traceOs.write(EVENT_TAG_RECORD);
            writeParam(thread, PARAM_J2EE_TRANSACTIONS, Long.toString(tmp));
            offs--;
        }
        if (callInfo.queueWaitDuration != 0) {
            traceOs.write(EVENT_TAG_RECORD);
            writeParam(thread, PARAM_QUEUE_WAIT_TIME, Integer.toString(callInfo.queueWaitDuration));
            offs--;
        }
        return offs;
    }

    private void writeParam(ThreadState thread, int id, String value) throws IOException {
        writeParam(thread, id, value, null);
    }

    private void writeParam(ThreadState thread,
                            int id, String value,
                            org.qubership.profiler.agent.BigValueHolder valueHolder) throws IOException {
        final IDataOutputStreamEx traceOs = this.traceOs.getStream();
        traceOs.writeVarInt(id);
        final int paramType = paramTypes.get(id);
        traceOs.write(paramType);
        switch (paramType) {
            case org.qubership.profiler.agent.ParamTypes.PARAM_INDEX:
                if (id != PARAM_JAVA_THREAD) {
                    THashSet<String> values = thread.params.get(id);
                    if (values == null) {
                        values = new THashSet<String>();
                        thread.params.put(id, values);
                    }
                    if (values.size() < MAX_VALUES_PER_INDEXED_PARAM)
                        values.add(value);
                }
            case org.qubership.profiler.agent.ParamTypes.PARAM_INLINE:
                traceOs.write(value);
                break;
            case org.qubership.profiler.agent.ParamTypes.PARAM_BIG_DEDUP:
                long hash = MurmurHash.hash64(value);
                long cached = dedupParamCache.get(hash);
                if (cached == -1) {
                    int offs = bigParamsDedupOs.getStream().write(value);
                    traceOs.writeVarInt(bigParamsDedupOs.getIndex());
                    traceOs.writeVarInt(offs);
                    if (valueHolder != null)
                        valueHolder.setAddress(bigParamsDedupOs.getIndex(), offs);
                    dedupParamCache.put(hash, ((long) bigParamsDedupOs.getIndex() << 32) | offs);
                } else {
                    traceOs.writeVarInt((int) (cached >> 32));
                    traceOs.writeVarInt((int) cached);
                    if (valueHolder != null)
                        valueHolder.setAddress((int) (cached >> 32), (int) cached);
                }
                break;
            case org.qubership.profiler.agent.ParamTypes.PARAM_BIG:
                if (value.length() > 10240) {
                    log.warn("Parameter larger than 10 KB is being recorded. Param id is {}. Param size is {}. First 1kb is {}", id, value.length(), value.substring(0, 1096));
                }
                int offset = bigParamsOs.getStream().write(value);
                traceOs.writeVarInt(bigParamsOs.getIndex());
                traceOs.writeVarInt(offset);
                if (valueHolder != null)
                    valueHolder.setAddress(bigParamsOs.getIndex(), offset);
                break;
        }
    }

    private void cleanupBuffer(LocalBuffer buffer) {
        buffer.state = null;
        //make sure a chain of previous buffers and a set of object params is not retained
        buffer.reset();
    }

    public void dumpDictionary() throws IOException {
        final List<String> tags = dictionary;
        final int size = tags.size();
        final IDataOutputStreamEx dictOs = this.dictOs.getStream();
        for (int i = lastWrittenDictionaryTag; i < size; i++) {
            dictOs.write(tags.get(i));

            this.dictOs.writePhrase();
        }
        lastWrittenDictionaryTag = size;
    }

    public void dumpSuspendLog() throws IOException {
        int i = TimerCache.lastLoggedEvent;
        long[] dates = TimerCache.suspendDates;
        int[] durations = TimerCache.suspendDurations;
        final IDataOutputStreamEx suspendOs = this.suspendOs.getStream();
        long prevTime = lastSuspendLogEntry;
        while (i != TimerCache.lastSuspendEvent) {
            long time = dates[i];
            int duration = durations[i];
            if (prevSuspendDate == time - duration) {
                prevSuspendDate = time;
                prevSuspendDuration += duration;

                inMemorySuspendLogBuilder.visitNotFinishedHiccup(prevSuspendDate, prevSuspendDuration);
            } else {
                if (prevSuspendDate != -1) {
                    inMemorySuspendLogBuilder.visitFinishedHiccup(prevSuspendDate, prevSuspendDuration);

                    suspendOs.writeVarInt((int) (prevSuspendDate - prevTime));
                    suspendOs.writeVarInt(prevSuspendDuration);

                    prevTime = prevSuspendDate;

                    this.suspendOs.writePhrase();
                }
                prevSuspendDate = time;
                prevSuspendDuration = duration;
            }
            i = i == TimerCache.SUSPEND_LOG_SIZE ? 0 : i + 1;
        }
        TimerCache.lastLoggedEvent = i;
        lastSuspendLogEntry = prevTime;
    }

    private void rotateDumpFile() throws IOException {
        for (int i = 0, outputStreamsSize = outputStreams.size(); i < outputStreamsSize; i++) {
            ICompressedLocalAndRemoteOutputStream stream = outputStreams.get(i);
            stream.rotate();
        }
    }

    private void flushDumpFile() throws IOException {
        for (int i = 0, outputStreamsSize = outputStreams.size(); i < outputStreamsSize; i++) {
            ICompressedLocalAndRemoteOutputStream stream = outputStreams.get(i);
            IDataOutputStreamEx os = stream.getStream();
            if (os != null)
                os.flush();
        }
        lastStreamFlushTime = TimerCache.timer;
    }

    private void close(Closeable os) {
        if (os != null) try {
            os.close();
        } catch (IOException e) {/**/}
    }

    public File getCurrentRoot() {
        return dumpRoot;
    }

    public long getRecordsWritten() {
        return recordsWritten;
    }

    public long getUncompressedSize() {
        long size = 0;
        for (int i = 0, outputStreamsSize = outputStreams.size(); i < outputStreamsSize; i++) {
            ICompressedLocalAndRemoteOutputStream stream = outputStreams.get(i);
            size += stream.getUncompressedSize();
        }
        return size;
    }

    public long getCompressedSize() {
        long size = 0;
        for (int i = 0, outputStreamsSize = outputStreams.size(); i < outputStreamsSize; i++) {
            ICompressedLocalAndRemoteOutputStream stream = outputStreams.get(i);
            size += stream.getCompressedSize();
        }
        return size;
    }

    public long getDumpTime() {
        return dumpTime;
    }

    public long getDumperStartTime() {
        return dumperStartTime;
    }

    public long getArchiveSize() {
        if (dumpFileManager == null) {
            return 0L;
        }
        return dumpFileManager.getCurrentSize();
    }

    public void forceRescanDumpDir() {
        if (dumpFileManager == null) {
            return;
        }
        dumpFileManager.rescan();
    }

    public DumperCallsExporter getDumperCallsExporter() {
        return dumperCallsExporter;
    }

    private class CallsCompressedLocalAndRemoteOutputStream extends CompressedLocalAndRemoteOutputStream {

        public CallsCompressedLocalAndRemoteOutputStream(String name, int rotateThreshold, int version) {
            super(name, rotateThreshold, version, new CallsState());
        }

        @Override
        public void fileRotated() throws IOException {
            CallsState callsState = (CallsState) getState();
            callsState.threadIdsCounter = 0;
            callsState.threadIdsCache.clear();
            long now = TimerCache.now;
            callsState.callsTimer = (int) (now - TimerCache.startTime);
            getStream().writeLong(now);
        }
    }
}
