package org.qubership.profiler.util;

import static org.qubership.profiler.agent.FilterOperator.*;

import org.qubership.profiler.ServerNameResolver;
import org.qubership.profiler.agent.*;
import org.qubership.profiler.dump.ThreadState;
import org.qubership.profiler.formatters.title.ProfilerTitle;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.procedure.TIntObjectProcedure;
import gnu.trove.set.hash.THashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;

public class DumperCallsExporter {
    private static final Logger log = LoggerFactory.getLogger(DumperCallsExporter.class);
    private static final String     PROFILER_BASE_URL = "/profiler/tree.html#params-trim-size=15000&";
    private static Map<String, Object> params = new HashMap<String, Object>(4);
    private static Map<String, String> additionalInputParams = new HashMap<String, String>(2);

    static {
        additionalInputParams.put(NODE_NAME_PARAM, String.valueOf(ServerNameResolver.SERVER_NAME));
        params.put(ADDITIONAL_INPUT_PARAMS, additionalInputParams);
    }

    private ArrayBlockingQueue<ByteArrayOutputStream> jsonsToSend;
    private ArrayBlockingQueue<ByteArrayOutputStream> emptyJsonBuffers;
    private List<String> includedParams;
    private List<String> excludedParams;
    private FilterOperator callFilter;
    private JsonFactory jsonFactory = new JsonFactory();
    private long missed = 0;
    private long prevMissed = 0;
    private long t1 = TimerCache.now;
    private long t2;
    private JsonGenerator jgen = null;
    private HashMap<String, String> callParams = new HashMap<String, String>();
    private HashSet<String> allowedParams = new HashSet<String>();
    List<String> dictionary = org.qubership.profiler.agent.ProfilerData.getTags();

    final TIntObjectProcedure<THashSet<String>> WRITE_PARAMS_JSON = new TIntObjectProcedure<THashSet<String>>() {
        public boolean execute(int id, THashSet<String> set) {
            try {
                String paramName = dictionary.get(id);
                if (includedParams.contains(paramName) || (includedParams.isEmpty() && !excludedParams.contains(paramName))) {
                    if (!set.isEmpty() && id < dictionary.size() && id > -1) {
                        if (set.size() == 1) {
                            jgen.writeObjectField(paramName, set.toArray()[0]);
                        } else {
                            if (set.size() > 1) {
                                jgen.writeFieldName(paramName);
                                jgen.writeStartArray();
                                for (String s : set)
                                    jgen.writeObject(s);
                                jgen.writeEndArray();
                            }
                        }
                    }
                }
            } catch (IOException e) {
                log.error("Error during writing into JSON", e);
            }

            return true;
        }
    };

    public synchronized void configureExport(ArrayBlockingQueue<ByteArrayOutputStream> jsonsToSend, ArrayBlockingQueue<ByteArrayOutputStream> emptyJsonBuffers, NetworkExportParams exportParams) {
        callParams.clear();
        this.jsonsToSend = jsonsToSend;
        this.emptyJsonBuffers = emptyJsonBuffers;
        if(exportParams == null) {
            this.includedParams = Collections.EMPTY_LIST;
            this.excludedParams = Collections.EMPTY_LIST;
            this.callFilter = null;
        }
        this.includedParams = exportParams.getIncludedParams() == null ? Collections.EMPTY_LIST : exportParams.getIncludedParams();
        this.excludedParams = exportParams.getExcludedParams() == null ? Collections.EMPTY_LIST : exportParams.getExcludedParams();
        this.callFilter = exportParams.getFilter();
        allowedParams.clear();
        if (this.includedParams.isEmpty()) {
            allowedParams.add("start.timestamp");
            allowedParams.add("profiler.title");
            allowedParams.add("node.name");
            allowedParams.add("java.thread");
            allowedParams.add("duration");
            allowedParams.add("time.suspension");
            allowedParams.add("time.queue.wait");
            allowedParams.add("time.concurrent.wait");
            allowedParams.add("time.cpu");
            allowedParams.add("method.name");
            allowedParams.add("log.generated");
            allowedParams.add("log.written");
            allowedParams.add("memory.allocated");
            allowedParams.add("io.disk.read");
            allowedParams.add("io.disk.written");
            allowedParams.add("io.net.read");
            allowedParams.add("io.net.written");
            allowedParams.add("j2ee.transactions");
            allowedParams.add("calls");
            allowedParams.add("profiler.url");
            allowedParams.removeAll(this.excludedParams);
        } else {
            allowedParams.addAll(this.includedParams);
        }

        for(String property : exportParams.getSystemProperties()) {
            callParams.put(property, System.getProperty(property));
            allowedParams.add(property);
        }
    }

    private ByteArrayOutputStream buildJson(long startTimestamp, long callDuration, long callSuspension, CallInfo callInfo, ProfilerTitle profilerTitle,
                                            ThreadState thread, String threadName, String dumpDir, ByteArrayOutputStream outputStream) throws IOException {
        jgen = jsonFactory.createGenerator(outputStream);
        final TIntObjectHashMap<THashSet<String>> params = thread.params;
        callParams.put("start.timestamp", String.valueOf(startTimestamp));
        callParams.put("profiler.title", String.valueOf(profilerTitle.getText()));
        callParams.put(NODE_NAME_PARAM, String.valueOf(ServerNameResolver.SERVER_NAME));
        callParams.put(THREAD_NAME_PARAM, String.valueOf(threadName));
        callParams.put("duration", String.valueOf(callDuration));
        callParams.put("time.suspension", String.valueOf(callSuspension));
        callParams.put("time.queue.wait", String.valueOf(callInfo.queueWaitDuration));
        callParams.put("time.concurrent.wait", String.valueOf(callInfo.waitTime - thread.prevWaitTime));
        callParams.put("time.cpu", String.valueOf(callInfo.cpuTime - thread.prevCpuTime));
        callParams.put("method.name", dictionary.get(thread.method));
        callParams.put("log.generated", String.valueOf(callInfo.logGenerated));
        callParams.put("log.written", String.valueOf(callInfo.logWritten));
        callParams.put("memory.allocated", String.valueOf(callInfo.memoryUsed - thread.prevMemoryUsed));
        callParams.put("io.disk.read", String.valueOf(callInfo.fileRead - thread.prevFileRead));
        callParams.put("io.disk.written", String.valueOf(callInfo.fileWritten - thread.prevFileWritten));
        callParams.put("io.net.read", String.valueOf(callInfo.netRead - thread.prevNetRead));
        callParams.put("io.net.written", String.valueOf(callInfo.netWritten - thread.prevNetWritten));
        callParams.put("j2ee.transactions", String.valueOf(callInfo.transactions - thread.prevTransactions));
        callParams.put("calls", String.valueOf(thread.calls));
        callParams.put("profiler.url", buildProfilerUrl(dumpDir, thread));

        jgen.writeStartObject();
        for (Map.Entry<String, String> entry : callParams.entrySet()) {
            if (allowedParams.contains(entry.getKey()))
                jgen.writeStringField(entry.getKey(), entry.getValue());
        }

        params.forEachEntry(WRITE_PARAMS_JSON);
        jgen.writeEndObject();
        if (jgen != null)
            jgen.close();
        return outputStream;
    }

    private String buildProfilerUrl(String dumpDir, ThreadState threadState) {
        String rowid = "0_" + threadState.traceFileIndex + "_" + threadState.bufferOffset + "_" + threadState.recordIndex + "_" + 0 + "_" + 0;
        return PROFILER_BASE_URL + "f[_0]=" + dumpDir + "&i=" + rowid;
    }

    public synchronized void exportCall(long startTimestamp, long callDuration, long callSuspension, CallInfo callInfo, ProfilerTitle profilerTitle, ThreadState threadState,
                                        String threadName, String dumpDir) {
        try {
            ByteArrayOutputStream baos;
            if (isEnabled()) {
                if(!filterCall(callDuration, callInfo, threadName, threadState)) {
                    return;
                }

                if ((baos = emptyJsonBuffers.poll()) != null) {
                    ByteArrayOutputStream baosForSend = buildJson(startTimestamp, callDuration, callSuspension, callInfo, profilerTitle, threadState, threadName, dumpDir, baos);
                    jsonsToSend.add(baosForSend);
                } else {
                    missed++;
                    t2 = TimerCache.now;
                }
                if (missed != prevMissed && (t2-t1)>60000) {
                    log.warn("{} jsons missed since startup", missed);
                    prevMissed = missed;
                    t1 = t2;
                }
            }
        } catch (Exception ex) {
            log.error("Error in exportCall: ", ex);
        }
    }

    public boolean isEnabled() {
        return jsonsToSend != null && emptyJsonBuffers != null;
    }

    private boolean filterCall(long callDuration, CallInfo callInfo, String threadName, ThreadState threadState) {
        if(callFilter == null) {
            return true;
        }
        params.put(CALL_INFO_PARAM, callInfo);
        params.put(THREAD_STATE_PARAM, threadState);
        params.put(DURATION_PARAM, callDuration);
        additionalInputParams.put(THREAD_NAME_PARAM, threadName);

        return callFilter.evaluate(params);
    }

}
