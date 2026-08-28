package com.netcracker.profiler.cli;

import static com.netcracker.profiler.cli.ExportDump.NUMBER_DIRECTORY_FILTER;
import static com.netcracker.profiler.cli.ExportDump.YEAR_DIRECTORY_FILTER;

import com.netcracker.profiler.dump.DataInputStreamEx;
import com.netcracker.profiler.dump.ParamTypes;
import com.netcracker.profiler.guice.DumpRootLocation;
import com.netcracker.profiler.io.DurationParser;
import com.netcracker.profiler.io.ParamReader;
import com.netcracker.profiler.io.ParamReaderFileFactory;
import com.netcracker.profiler.sax.readers.ProfilerTraceReader;
import com.netcracker.profiler.tags.Dictionary;

import net.sourceforge.argparse4j.inf.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.ZipException;

import jakarta.inject.Inject;

public class DumpTrace implements Command {
    private static final Logger log = LoggerFactory.getLogger(DumpTrace.class);

    private final File dumpRoot;
    private final ParamReaderFileFactory paramReaderFileFactory;

    @Inject
    public DumpTrace(@DumpRootLocation File dumpRoot, ParamReaderFileFactory paramReaderFileFactory) {
        this.dumpRoot = dumpRoot;
        this.paramReaderFileFactory = paramReaderFileFactory;
    }

    @Override
    public int accept(Namespace args) {
        if (dumpRoot == null) {
            log.warn("No dump path found. Please check path to ESC dump (--dump-root)");
            return -2;
        }

        TimeZone timeZone = TimeZone.getTimeZone(args.getString("time_zone"));
        long endDate = DurationParser.parseTimeInstant(args.getString("end_date"), Long.MAX_VALUE, Long.MAX_VALUE, timeZone);
        long startDate = DurationParser.parseTimeInstant(args.getString("start_date"), Long.MAX_VALUE, endDate, timeZone);
        List<String> selectedServers = args.getList("server");
        String contains = args.getString("contains");
        String outputFile = args.getString("output_file");

        Set<String> servers = selectedServers == null || selectedServers.isEmpty() ? null : new HashSet<>(selectedServers);
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS z");
        dateFormat.setTimeZone(timeZone);

        boolean closeOutput = !"-".equals(outputFile);
        try {
            OutputStream os = closeOutput ? new FileOutputStream(outputFile) : System.out;
            PrintWriter out = new PrintWriter(new OutputStreamWriter(os, StandardCharsets.UTF_8));
            try {
                dumpTrace(startDate, endDate, servers, contains, dateFormat, out);
            } finally {
                out.flush();
                if (closeOutput) {
                    out.close();
                }
            }
            return 0;
        } catch (Exception e) {
            log.error("Unable to dump raw trace", e);
            return -1;
        }
    }

    private void dumpTrace(
            long startDate,
            long endDate,
            Set<String> servers,
            String contains,
            SimpleDateFormat dateFormat,
            PrintWriter out) throws IOException {
        out.println("root\ttraceFileIndex\tbufferOffset\tthreadId\trecordIndex\tdepth\teventType\teventTime\teventTimeMillis\ttagId\ttag\tvalue");

        List<File> serverDirs = listServerDirs(servers);
        for (File serverDir : serverDirs) {
            List<File> dumpDirs = new ArrayList<>();
            collectDumpDirs(serverDir, 0, startDate, endDate, dumpDirs);
            Collections.sort(dumpDirs);
            for (File dumpDir : dumpDirs) {
                dumpTraceDir(dumpDir, contains, dateFormat, out);
            }
        }
    }

    private List<File> listServerDirs(Set<String> servers) {
        File[] files = dumpRoot.listFiles(File::isDirectory);
        if (files == null) {
            return Collections.emptyList();
        }
        Arrays.sort(files);
        List<File> result = new ArrayList<>(files.length);
        for (File file : files) {
            if (servers != null && !servers.contains(file.getName())) {
                continue;
            }
            result.add(file);
        }
        return result;
    }

    private void collectDumpDirs(File root, int level, long startDate, long endDate, List<File> result) {
        if (level == 4) {
            long dumpTimestamp;
            try {
                dumpTimestamp = Long.parseLong(root.getName());
            } catch (NumberFormatException e) {
                return;
            }
            if (dumpTimestamp < startDate || dumpTimestamp > endDate) {
                return;
            }
            if (new File(root, "trace").exists()) {
                result.add(root);
            }
            return;
        }

        File[] files = root.listFiles(level == 0 ? YEAR_DIRECTORY_FILTER : NUMBER_DIRECTORY_FILTER);
        if (files == null) {
            return;
        }
        Arrays.sort(files);
        for (File file : files) {
            collectDumpDirs(file, level + 1, startDate, endDate, result);
        }
    }

    private void dumpTraceDir(
            File dumpDir,
            String contains,
            SimpleDateFormat dateFormat,
            PrintWriter out) throws IOException {
        File traceDir = new File(dumpDir, ProfilerTraceReader.TRACE_STREAM_NAME);
        File[] traceFiles = traceDir.listFiles(File::isFile);
        if (traceFiles == null || traceFiles.length == 0) {
            return;
        }
        Arrays.sort(traceFiles);

        String rootReference = dumpDir.getAbsolutePath().substring(dumpRoot.getAbsolutePath().length() + 1);
        List<TraceEvent> events = new ArrayList<>();
        BitSet requiredIds = new BitSet();

        int traceFileIndex = 1;
        DataInputStreamEx trace = null;
        try {
            while (true) {
                try {
                    trace = DataInputStreamEx.reopenDataInputStream(trace, dumpDir, ProfilerTraceReader.TRACE_STREAM_NAME, traceFileIndex);
                } catch (FileNotFoundException e) {
                    break;
                }
                if (trace == null) {
                    break;
                }
                readTraceFile(rootReference, trace, traceFileIndex, events, requiredIds);
                traceFileIndex++;
            }
        } finally {
            if (trace != null) {
                trace.close();
            }
        }

        ParamReader paramReader = paramReaderFileFactory.create(dumpDir);
        Dictionary tags = paramReader.fillTags(requiredIds, new ArrayList<Throwable>());
        for (TraceEvent event : events) {
            event.tag = event.tagId >= 0 && event.tagId < tags.size() ? tags.get(event.tagId) : null;
        }

        for (TraceEvent event : events) {
            String printable = event.tag == null ? "" : event.tag;
            String lineValue = printable;
            if (event.value != null && !event.value.isEmpty()) {
                lineValue = lineValue.isEmpty() ? event.value : lineValue + "=" + event.value;
            }
            if (contains != null && !(contains(lineValue, contains) || contains(event.eventType, contains))) {
                continue;
            }

            out.print(escape(rootReference));
            out.print('\t');
            out.print(event.traceFileIndex);
            out.print('\t');
            out.print(event.bufferOffset);
            out.print('\t');
            out.print(event.threadId);
            out.print('\t');
            out.print(event.recordIndex);
            out.print('\t');
            out.print(event.depth);
            out.print('\t');
            out.print(event.eventType);
            out.print('\t');
            out.print(escape(dateFormat.format(new Date(event.eventTimeMillis))));
            out.print('\t');
            out.print(event.eventTimeMillis);
            out.print('\t');
            out.print(event.tagId);
            out.print('\t');
            out.print(escape(event.tag));
            out.print('\t');
            out.println(escape(event.value));
        }
    }

    private void readTraceFile(String rootReference,
                               DataInputStreamEx trace,
                               int traceFileIndex,
                               List<TraceEvent> events,
                               BitSet requiredIds) throws IOException {
        long timerStartTime = trace.readLong();
        while (true) {
            int bufferOffset = trace.position();
            long threadId;
            try {
                threadId = trace.readLong();
            } catch (EOFException eof) {
                return;
            } catch (ZipException zipException) {
                if ("invalid stored block lengths".equals(zipException.getMessage())
                        || "Corrupt GZIP trailer".equals(zipException.getMessage())) {
                    return;
                }
                throw zipException;
            }

            long realTime = trace.readLong();
            int realTimeOffset = (int) (realTime - timerStartTime);
            int eventTime = -realTimeOffset;
            int depth = 0;

            for (int idx = 0; ; idx++) {
                int header = trace.read();
                int typ = header & 0x3;
                if (typ == ProfilerTraceReader.DumperConstants.EVENT_FINISH_RECORD) {
                    events.add(new TraceEvent(rootReference, traceFileIndex, bufferOffset, threadId, idx, depth,
                            "finish", realTime + eventTime, -1, null));
                    break;
                }

                int time = (header & 0x7f) >> 2;
                if ((header & 0x80) > 0) {
                    time |= trace.readVarInt() << 5;
                }
                eventTime += time;
                long eventRealTime = eventTime + realTime;

                int tagId = -1;
                String value = null;
                if (typ != ProfilerTraceReader.DumperConstants.EVENT_EXIT_RECORD) {
                    tagId = trace.readVarInt();
                    requiredIds.set(tagId);
                    if (typ == ProfilerTraceReader.DumperConstants.EVENT_TAG_RECORD) {
                        int paramType = trace.read();
                        value = readValue(trace, paramType);
                    }
                }

                if (typ == ProfilerTraceReader.DumperConstants.EVENT_ENTER_RECORD) {
                    depth++;
                    events.add(new TraceEvent(rootReference, traceFileIndex, bufferOffset, threadId, idx, depth,
                            "enter", eventRealTime, tagId, value));
                } else if (typ == ProfilerTraceReader.DumperConstants.EVENT_EXIT_RECORD) {
                    events.add(new TraceEvent(rootReference, traceFileIndex, bufferOffset, threadId, idx, depth,
                            "exit", eventRealTime, tagId, value));
                    depth = Math.max(0, depth - 1);
                } else if (typ == ProfilerTraceReader.DumperConstants.EVENT_TAG_RECORD) {
                    events.add(new TraceEvent(rootReference, traceFileIndex, bufferOffset, threadId, idx, depth,
                            "tag", eventRealTime, tagId, value));
                }
            }
        }
    }

    private String readValue(DataInputStreamEx trace, int paramType) throws IOException {
        switch (paramType) {
            case ParamTypes.PARAM_INDEX:
            case ParamTypes.PARAM_INLINE:
                return trace.readString();
            case ParamTypes.PARAM_BIG:
                return "xml:" + trace.readVarInt() + "/" + trace.readVarInt();
            case ParamTypes.PARAM_BIG_DEDUP:
                return "sql:" + trace.readVarInt() + "/" + trace.readVarInt();
            default:
                return "paramType=" + paramType;
        }
    }

    private boolean contains(String text, String needle) {
        return text != null && text.contains(needle);
    }

    private String escape(String value) {
        if (value == null) {
            return "";
        }
        return value
                .replace("\\", "\\\\")
                .replace("\t", "\\t")
                .replace("\r", "\\r")
                .replace("\n", "\\n");
    }

    private static final class TraceEvent {
        private final String rootReference;
        private final int traceFileIndex;
        private final int bufferOffset;
        private final long threadId;
        private final int recordIndex;
        private final int depth;
        private final String eventType;
        private final long eventTimeMillis;
        private final int tagId;
        private String tag;
        private final String value;

        private TraceEvent(
                String rootReference,
                int traceFileIndex,
                int bufferOffset,
                long threadId,
                int recordIndex,
                int depth,
                String eventType,
                long eventTimeMillis,
                int tagId,
                String value) {
            this.rootReference = rootReference;
            this.traceFileIndex = traceFileIndex;
            this.bufferOffset = bufferOffset;
            this.threadId = threadId;
            this.recordIndex = recordIndex;
            this.depth = depth;
            this.eventType = eventType;
            this.eventTimeMillis = eventTimeMillis;
            this.tagId = tagId;
            this.value = value;
        }
    }
}
