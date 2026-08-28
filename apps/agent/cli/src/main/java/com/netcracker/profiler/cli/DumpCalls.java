package com.netcracker.profiler.cli;

import com.netcracker.profiler.configuration.ParameterInfoDto;
import com.netcracker.profiler.guice.DumpRootLocation;
import com.netcracker.profiler.io.*;
import com.netcracker.profiler.tags.Dictionary;

import net.sourceforge.argparse4j.inf.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@Singleton
public class DumpCalls implements Command {
    private static final Logger log = LoggerFactory.getLogger(DumpCalls.class);

    private final File dumpRoot;
    private final CallReaderFactory callReaderFactory;

    @Inject
    public DumpCalls(@DumpRootLocation File dumpRoot, CallReaderFactory callReaderFactory) {
        this.dumpRoot = dumpRoot;
        this.callReaderFactory = callReaderFactory;
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
        long minDuration = args.getInt("min_duration");
        long maxDuration = args.getLong("max_duration");
        String methodContains = args.getString("method_contains");
        boolean includeParams = args.getBoolean("include_params");
        List<String> selectedServers = args.getList("server");

        if (startDate > System.currentTimeMillis()) {
            log.error("--start-date is in the future. Please clarify the arguments and retry.");
            return -1;
        }

        String outputFile = args.getString("output_file");
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS z");
        dateFormat.setTimeZone(timeZone);

        long now = System.currentTimeMillis();
        TemporalRequestParams temporal = new TemporalRequestParams(now, now, now, startDate, endDate, 0, minDuration, maxDuration);
        Set<String> nodes = selectedServers == null || selectedServers.isEmpty() ? null : new HashSet<>(selectedServers);
        Map<String, String[]> params = Collections.emptyMap();

        boolean closeOutput = !"-".equals(outputFile);
        try {
            OutputStream os = closeOutput ? new FileOutputStream(outputFile) : System.out;
            PrintWriter out = new PrintWriter(new OutputStreamWriter(os, StandardCharsets.UTF_8));
            try {
                dumpCalls(temporal, params, nodes, methodContains, includeParams, dateFormat, out);
            } finally {
                out.flush();
                if (closeOutput) {
                    out.close();
                }
            }
            return 0;
        } catch (Exception e) {
            log.error("Unable to dump calls", e);
            return -1;
        }
    }

    private void dumpCalls(
            TemporalRequestParams temporal,
            Map<String, String[]> params,
            Set<String> nodes,
            String methodContains,
            boolean includeParams,
            SimpleDateFormat dateFormat,
            PrintWriter out) throws IOException {
        out.println("root\tstart\tstartMillis\tdurationMs\tcalls\tmethod\tthread\ttraceFileIndex\tbufferOffset\trecordIndex\ttransactions\tsuspendMs\tqueueWaitMs\tlogsGenerated\tlogsWritten\tcpuTime\twaitTime\tmemoryUsed\tfileRead\tfileWritten\tnetRead\tnetWritten\tparams");

        TextCallListener listener = new TextCallListener(out, dateFormat, methodContains, includeParams);
        List<ICallReader> readers = callReaderFactory.collectCallReaders(
                params,
                temporal,
                listener,
                new DurationFiltererImpl(temporal.durationFrom, temporal.durationTo),
                nodes
        );

        List<Throwable> exceptions = new ArrayList<>();
        for (ICallReader reader : readers) {
            reader.find();
            exceptions.addAll(reader.getExceptions());
        }
        for (Throwable exception : exceptions) {
            log.error("Exception while reading calls", exception);
        }
    }

    private static final class TextCallListener implements CallListener {
        private final PrintWriter out;
        private final SimpleDateFormat dateFormat;
        private final String methodContains;
        private final boolean includeParams;

        private TextCallListener(PrintWriter out, SimpleDateFormat dateFormat, String methodContains, boolean includeParams) {
            this.out = out;
            this.dateFormat = dateFormat;
            this.methodContains = methodContains;
            this.includeParams = includeParams;
        }

        @Override
        public void processCalls(
                String rootReference,
                ArrayList<Call> calls,
                Dictionary tags,
                Map<String, ParameterInfoDto> paramInfo,
                BitSet requiredIds) {
            for (Call call : calls) {
                String method = safeTag(tags, call.method);
                if (methodContains != null && !method.contains(methodContains)) {
                    continue;
                }
                out.print(escape(rootReference));
                out.print('\t');
                out.print(escape(dateFormat.format(new Date(call.time))));
                out.print('\t');
                out.print(call.time);
                out.print('\t');
                out.print(call.duration);
                out.print('\t');
                out.print(call.calls);
                out.print('\t');
                out.print(escape(method));
                out.print('\t');
                out.print(escape(call.threadName));
                out.print('\t');
                out.print(call.traceFileIndex);
                out.print('\t');
                out.print(call.bufferOffset);
                out.print('\t');
                out.print(call.recordIndex);
                out.print('\t');
                out.print(call.transactions);
                out.print('\t');
                out.print(call.suspendDuration);
                out.print('\t');
                out.print(call.queueWaitDuration);
                out.print('\t');
                out.print(call.logsGenerated);
                out.print('\t');
                out.print(call.logsWritten);
                out.print('\t');
                out.print(call.cpuTime);
                out.print('\t');
                out.print(call.waitTime);
                out.print('\t');
                out.print(call.memoryUsed);
                out.print('\t');
                out.print(call.fileRead);
                out.print('\t');
                out.print(call.fileWritten);
                out.print('\t');
                out.print(call.netRead);
                out.print('\t');
                out.print(call.netWritten);
                out.print('\t');
                out.println(escape(formatParams(call, tags, paramInfo, includeParams)));
            }
        }

        private String formatParams(Call call, Dictionary tags, Map<String, ParameterInfoDto> paramInfo, boolean includeParams) {
            if (!includeParams || call.params == null || call.params.isEmpty()) {
                return "";
            }
            Map<String, List<String>> result = new TreeMap<>();
            for (Map.Entry<Integer, List<String>> entry : call.params.entrySet()) {
                String key = safeTag(tags, entry.getKey());
                ParameterInfoDto info = paramInfo.get(key);
                if (info != null && info.name != null) {
                    key = info.name;
                }
                result.put(key, entry.getValue());
            }
            List<String> items = new ArrayList<>(result.size());
            for (Map.Entry<String, List<String>> entry : result.entrySet()) {
                items.add(entry.getKey() + "=" + String.join("|", entry.getValue()));
            }
            return String.join(";", items);
        }

        private String safeTag(Dictionary tags, int tagId) {
            String value = tags.get(tagId);
            return value == null ? "#" + tagId : value;
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
    }
}
