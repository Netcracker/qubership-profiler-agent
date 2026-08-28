package com.netcracker.profiler.io;

import com.netcracker.profiler.chart.UnaryFunction;
import com.netcracker.profiler.configuration.ParameterInfoDto;
import com.netcracker.profiler.dump.DataInputStreamEx;
import com.netcracker.profiler.dump.DumperDetector;
import com.netcracker.profiler.guice.DumpRootLocation;
import com.netcracker.profiler.io.call.CallDataReaderFactory;
import com.netcracker.profiler.sax.factory.SuspendLogFactory;
import com.netcracker.profiler.tags.Dictionary;
import com.netcracker.profiler.tags.DictionaryList;
import com.netcracker.profiler.timeout.ProfilerTimeoutHandler;
import com.netcracker.profiler.util.ProfilerConstants;
import com.netcracker.profiler.utils.CommonUtils;

import com.google.inject.assistedinject.Assisted;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.ZipException;

import jakarta.inject.Inject;

/**
 * Prototype-scoped class - create instances via {@code CallReaderFileFactory}.
 */
public class CallReaderFile extends CallReader {
    public static final boolean READ_CALL_RANGES = Boolean.parseBoolean(System.getProperty("com.netcracker.profiler.Profiler.READ_CALL_RANGES", "true"));
    public static final boolean READ_CALLS_DICTIONARY = Boolean.parseBoolean(System.getProperty("com.netcracker.profiler.Profiler.READ_CALLS_DICTIONARY", "true"));
    public static final boolean USE_FAST_CALL_READER = Boolean.parseBoolean(System.getProperty("profiler.USE_FAST_CALL_READER", "false"));
    public static final int CALLS_SCANNER_UPPER_BOUND_MINUTES = Integer.getInteger("profiler.CALLS_SCANNER_UPPER_BOUND_MINUTES", 60);
    private final static Logger logger = LoggerFactory.getLogger(CallReaderFile.class);

    private final File rootFile;
    private final ParamReaderFileFactory paramReaderFileFactory;
    private final SuspendLogFactory suspendLogFactory;
    private final CallDataReaderFactory callDataReaderFactory;

    private File inFlightRoot;
    private String inFlightRootPath;
    private Set<Call> inflightCalls;

    private String beginPath;
    private String endPath;

    private final @Nullable Set<String> nodes;
    private final @Nullable Set<String> dumpDirs;
    private boolean readDictionary = true;
    private boolean useCallsDictionary;

    private long durationFrom = 0;
    private long durationTo = Long.MAX_VALUE;

    private final static UnaryFunction<File, Long> CALLS_START_TIMESTAMP = new UnaryFunction<File, Long>() {
        public Long evaluate(File file) {
            try {
                DataInputStreamEx calls = DataInputStreamEx.openDataInputStream(file, 16);
                long time = calls.readLong();
                if ((int) (time >>> 32) == ProfilerConstants.CALL_HEADER_MAGIC) {
                    time = calls.readLong();
                }
                if (logger.isTraceEnabled()) {
                    logger.trace("Timestamp of {} is {} ({})"
                            , new Object[]{file.getAbsolutePath(), new Date(time), time}
                    );
                }
                return time;
            } catch (EOFException e) {
                return System.currentTimeMillis();
            } catch (ZipException e) {
                if(!"invalid stored block lengths".equals(e.getMessage())) {
                    //it's ok to get ZipException(invalid stored block lengths) when reading current stream
                    throw new RuntimeException(e);
                }
                return System.currentTimeMillis();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    };

    private final static Comparator<Long> LONG_COMPARATOR = new Comparator<Long>() {
        public int compare(Long o1, Long o2) {
            return o1.compareTo(o2);
        }
    };

    /**
     * @see CallReaderFileFactory
     */
    @Inject
    public CallReaderFile(
            @Assisted("callback") CallListener callback,
            @Assisted("filterer") CallFilterer cf,
            @Nullable @Assisted("nodes") Set<String> nodes,
            @Assisted("readDictionary") boolean readDictionary,
            @Nullable @Assisted("dumpDirs") Set<String> dumpDirs,
            @DumpRootLocation File rootFile,
            ParamReaderFileFactory paramReaderFileFactory,
            SuspendLogFactory suspendLogFactory,
            CallDataReaderFactory callDataReaderFactory) {
        super(callback, cf);
        this.nodes = nodes;
        this.readDictionary = readDictionary;
        this.dumpDirs = dumpDirs;
        this.rootFile = rootFile;
        this.paramReaderFileFactory = paramReaderFileFactory;
        this.suspendLogFactory = suspendLogFactory;
        this.callDataReaderFactory = callDataReaderFactory;
        if(cf instanceof DurationFilterer) {
            DurationFilterer durationFilterer = (DurationFilterer) cf;
            this.durationFrom = durationFilterer.getDurationFrom();
            this.durationTo = durationFilterer.getDurationTo();
        }
    }

    @Override
    protected CallDataReaderFactory getCallDataReaderFactory() {
        return callDataReaderFactory;
    }

    protected void innerFind() {
        Object[] inFlights = paramReaderFileFactory.create(null).getInflightCalls();
        if (inFlights != null) {
            inFlightRoot = (File) inFlights[0];
            inFlightRootPath = inFlightRoot.getAbsolutePath();
            inflightCalls = new HashSet<Call>((List<Call>) inFlights[1]);
        }

        SimpleDateFormat sdf = new SimpleDateFormat("'" + File.separatorChar + "'yyyy'" + File.separatorChar + "'MM'" + File.separatorChar + "'dd");
        beginPath = begin == Long.MIN_VALUE ? null : sdf.format(begin) + File.separatorChar + begin;
        endPath = end == Long.MAX_VALUE ? null : sdf.format(end) + File.separatorChar + end;
        findInFolder(rootFile, "", 0);

        try {
            findInMemory();
        } catch (Exception e) {
            logger.error("Skipping inflight calls. Reason: {}", e.getMessage());
        }
    }

    private boolean findCallsInFile(File root, SuspendLog suspendLog, ArrayList<Call> result, final BitSet requiredIds, long endScan) {
        DataInputStreamEx callsStream = null;
        try {
            DataInputStreamEx calls = callsStream = DataInputStreamEx.openDataInputStream(root);
            return findCallsInStream(calls, null, suspendLog, result, requiredIds, endScan);
        } catch (FileNotFoundException e) {
            exceptions.add(e);
        } catch (IOException e) {
            exceptions.add(e);
        } finally {
            close(callsStream);
        }
        return false;
    }

    private void close(DataInputStreamEx calls) {
        if (calls != null)
            try {
                calls.close();
            } catch (IOException e) {
                /**/
            }
    }

    private String getJSReference(File dumpRoot) {
        String dumpDir = dumpRoot.getAbsolutePath(); // == profiler/dump/server_name/2010/06/10/123123123
        dumpRoot = dumpRoot.getParentFile().getParentFile().getParentFile().getParentFile().getParentFile(); // profiler/dump/server_name
        return dumpDir.substring(dumpRoot.getAbsolutePath().length() + 1); // 2010/06/10/123123123
    }

    private void findInFolder(File root, String currentPath, int level) {
        if (level != 0 && endPath != null && currentPath.compareTo(endPath) > 0)
            return;
        if(level == 1 && nodes != null && !nodes.contains(root.getName())) {
            return;
        }
        if (level == 5) {
            /* We are at root/2010/04/24/123342342 */
            if(dumpDirs != null && !dumpDirs.contains(root.getAbsolutePath()) && !dumpDirs.contains(getRelativePath(root))) {
                return;
            }
            if(!(new File(root, "calls")).exists()) {
                return;
            }

            SuspendLog suspendLog;
            try {
                suspendLog = suspendLogFactory.readMultiRangeSuspendLog(root.getAbsolutePath(), beginSuspendLog, endSuspendLog);
            } catch (IOException e) {
                suspendLog = SuspendLog.EMPTY;
                exceptions.add(e);
            }

            BitSet requiredIds = new BitSet();
            final ParamReader paramReader = paramReaderFileFactory.create(root);
            Map<String, ParameterInfoDto> paramInfo = paramReader.fillParamInfo(exceptions, root.getAbsolutePath());
            Dictionary tags;
            File callsDictionaryFolder = new File(root, "callsDictionary");
            useCallsDictionary = false;
            if(READ_CALLS_DICTIONARY && callsDictionaryFolder.exists()) {
                tags = paramReader.fillCallsTags(exceptions);
                useCallsDictionary = true;
            } else {
                tags = new DictionaryList();
            }


            TreeMap<Long, File> callRangeFoldersMap = findCallRangeFolders(root);
            if(!READ_CALL_RANGES || callRangeFoldersMap.isEmpty() || durationFrom < callRangeFoldersMap.firstKey()) {
                File callsFolder = new File(root, "calls");
                if (!callsFolder.exists()) return;
                long endScan = Long.MAX_VALUE;
                if(USE_FAST_CALL_READER) {
                    endScan = end + (CALLS_SCANNER_UPPER_BOUND_MINUTES * 60 * 1000);
                    if(endScan < 0) { //Overflow
                        endScan = Long.MAX_VALUE;
                    }
                }
                findInCallsFolder(callsFolder, suspendLog, requiredIds, paramInfo, tags, root, endScan, paramReader);
            } else {
                long endScan = Long.MAX_VALUE;
                long maxDuration = Long.MAX_VALUE;
                for(Map.Entry<Long, File> callsFolderEntry : callRangeFoldersMap.descendingMap().entrySet()) {
                    long minDuration = callsFolderEntry.getKey();
                    if(minDuration > durationTo) {
                        continue;
                    }
                    if(maxDuration < durationFrom) {
                        break;
                    }

                    File callsFolder = callsFolderEntry.getValue();
                    findInCallsFolder(callsFolder, suspendLog, requiredIds, paramInfo, tags, root, endScan, paramReader);
                    endScan = end + maxDuration;
                    if(endScan < 0) { //Overflow
                        endScan = Long.MAX_VALUE;
                    }
                    maxDuration = minDuration - 1;
                }
            }

            // TODO: is it required?
            // callback.postProcess(getJSReference(root));
            return;
        }
        if (root.isDirectory()) {
            final File[] files = root.listFiles(CALLS_FILE_FINDER);
            if(files == null) {
                return;
            }
            Arrays.sort(files, Collections.reverseOrder());
            String endPath = null;
            if (level == 0)
                endPath = this.endPath;
            for (File f : files) {
                final String fileName = f.getName();
                if (level == 1 && fileName.length() != 4)
                    continue; // Should be year at level 1

                final String nextPath = currentPath + File.separatorChar + fileName;
                if (level == 0) {
                    if (endPath != null) // adjust this.endPath for the filtering to work
                        this.endPath = nextPath + endPath;
                    callBeginTime = Long.MAX_VALUE;
                    minCallBeginTime = Long.MAX_VALUE;
                }

                if (level > 0 && begin != Long.MIN_VALUE && minCallBeginTime < begin)
                    break;
                findInFolder(f, nextPath, level + 1);
            }
        }
    }

    private TreeMap<Long, File> findCallRangeFolders(File root) {
        TreeMap<Long, File> callRangeFolders = new TreeMap<>();
        for(File file : root.listFiles()) {
            String fileName = file.getName();
            if(!fileName.startsWith("calls[")) {
                continue;
            }
            int delimiterPos = fileName.indexOf('-');
            if(delimiterPos == -1) {
                delimiterPos = fileName.indexOf('+');
            }
            String startDurationStr = fileName.substring(6, delimiterPos);
            long minDuration = DurationParser.parseDuration(startDurationStr, -1);
            if(minDuration == -1) {
                logger.error("Incorrect calls range folder "+fileName+". Will skip scan of calls range files.");
                callRangeFolders.clear();
                return callRangeFolders;
            }
            callRangeFolders.put(minDuration, file);
        }
        return callRangeFolders;
    }

    private void findInCallsFolder(File callsFolder, SuspendLog suspendLog, BitSet requiredIds, Map<String, ParameterInfoDto> paramInfo,
                                   Dictionary tags, File root, long endScan, ParamReader paramReader) {
        final File[] files = callsFolder.listFiles();
        Arrays.sort(files);

        ArrayList<Call> result = new ArrayList<Call>();
        int prevCardinality = requiredIds.cardinality();
        int startIdx = getStartFileIndexByStartTime(files);
        for (int i = startIdx; i<files.length; i++) {
            ProfilerTimeoutHandler.checkTimeout();
            File f = files[i];
            final String fileName = f.getName();
            if (fileName.length() != 6 && fileName.length() != 9) continue;
            result.clear();

            boolean stop = findCallsInFile(f, suspendLog, result, requiredIds, endScan);
            if (result.isEmpty()) {
                if(stop) {
                    break;
                } else {
                    continue;
                }
            }

            int newCardinality = requiredIds.cardinality();
            if (prevCardinality != newCardinality && readDictionary) {
                prevCardinality = newCardinality;
                if (useCallsDictionary) {
                    for (int j = requiredIds.nextSetBit(0); j >= 0; j = requiredIds.nextSetBit(j + 1)) {
                        if (tags.get(j) == null) {
                            logger.warn("Cannot find tag {} in callsDictionary under {}. Will use full dictionary.", j, callsFolder.getParent());
                            useCallsDictionary = false;
                            break;
                        }
                    }
                }
                if (!useCallsDictionary) {
                    // TODO: MAKE SURE THAT IT WORKS!!!
                    tags.asList().clear();
                    tags.asList().addAll(paramReader.fillTags(requiredIds, exceptions).asList());
                    loadTags(tags, requiredIds, paramReader, "profiler.title");
                }
            }

            callDataReader.postCompute(result, tags, requiredIds);

            callback.processCalls(getJSReference(root), result, tags, paramInfo, requiredIds);

            if(stop) break;
        }
    }

    private void loadTags(Dictionary tags, BitSet requiredIds, ParamReader paramReader, String... tagNames) {
        ArrayList<String> tagsList = (ArrayList<String>) tags.asList();
        Integer[] tagIds = paramReader.findTags(exceptions, tagNames);
        for (int i = 0; i < tagNames.length; i++) {
            Integer tagId = tagIds[i];
            if (tagId == null) continue;
            String tagName = tagNames[i];
            tagsList.ensureCapacity(tagId);
            tagsList.set(tagId, tagName);
            requiredIds.set(tagId);
        }
    }

    private int getStartFileIndexByStartTime(File[] files) {
        //   * The method is guaranteed to return the maximal index of the element that is
        //   * less or equal to the given key.
        int from = CommonUtils.upperBound(files, begin, 0, files.length - 1, CALLS_START_TIMESTAMP, LONG_COMPARATOR);
        if (from == files.length) {
            from--;
        }
        from = Math.max(from, 0);

        return from;
    }

    private boolean checkDumper(){
        return  DumperDetector.dumperActive();
    }

    private void findInMemory() {
        if (inflightCalls == null) return;
        if (!checkDumper()) return;

        ArrayList<Call> result = new ArrayList<Call>(inflightCalls.size());
        BitSet requiredIds = new BitSet();

        SuspendLog suspendLog;
        try {
            suspendLog = suspendLogFactory.readSuspendLog(inFlightRoot.getAbsolutePath());
        } catch (IOException e) {
            suspendLog = SuspendLog.EMPTY;
            exceptions.add(e);
        }

        for (Call call : inflightCalls) {
            if (cf != null && !cf.filter(call) || (call.time > end) || (call.time + call.duration < begin))
                continue;

            call.setSuspendDuration(suspendLog.getSuspendDuration(call.time, call.time + call.duration));

            result.add(call);
            requiredIds.set(call.method);
            if (call.params != null)
                for (Integer id : call.params.keySet())
                    if (id > 0)
                        requiredIds.set(id);
        }

        if (result.isEmpty()) return;

        final Dictionary tags = paramReaderFileFactory.create(inFlightRoot).fillTags(requiredIds, exceptions);
        final Map<String, ParameterInfoDto> paramInfo = paramReaderFileFactory.create(inFlightRoot).fillParamInfo(exceptions, inFlightRoot.getAbsolutePath());
        callback.processCalls(getJSReference(inFlightRoot), result, tags, paramInfo, requiredIds);
    }

    private String getRelativePath(File dumpDirFile) {
        return dumpDirFile.getAbsolutePath().substring(rootFile.getAbsolutePath().length() + 1); // server_name/2010/06/10/123123123
}
}
