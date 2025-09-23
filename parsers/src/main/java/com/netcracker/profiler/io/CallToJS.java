package com.netcracker.profiler.io;

import com.netcracker.profiler.configuration.ParameterInfoDto;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Component
@Scope("prototype")
@Profile("filestorage")
public class CallToJS implements CallListener {
    private final static Logger log = LoggerFactory.getLogger(CallToJS.class);
    protected final PrintWriter out;
    String prevDumpDir;
    BitSet prevIds = new BitSet();

    @Value("${com.netcracker.profiler.DUMP_ROOT_LOCATION:#{null}}")
    private File rootFile;
    private CallFilterer cf;

    private static class DeferredCalls{
        List<Call> calls;
        List<String> tags;
        Map<String, ParameterInfoDto> paramInfo;
        BitSet requredIds;

        private DeferredCalls(List<Call> calls, List<String> tags, Map<String, ParameterInfoDto> paramInfo, BitSet requredIds) {
            this.calls = calls;
            this.tags = tags;
            this.paramInfo = paramInfo;
            this.requredIds = requredIds;
        }
    }

    //rootReference (pod_name) -> list of deferred calls
    //different root references may be processed in different threads
    private Map<String, DeferredCalls> deferredCalls = new ConcurrentHashMap<>();

    private CallToJS() {
        throw new RuntimeException("No-args not supported");
    }

    public CallToJS(PrintWriter out, CallFilterer cf) {
        this.out = out;
        this.cf = cf;
    }

    protected void printAdditionalRootReferenceDetails(String rootReference) throws IOException {
        Properties properties = new Properties();

        if (rootFile == null) {
            log.warn("Cannot find root file of dump");

            return;
        }

        File metaInfFile = new File(rootFile.getPath() + "/" + rootReference + "/meta-inf.properties");

        if (metaInfFile.exists()) {
            try (FileInputStream fileInputStream = new FileInputStream(metaInfFile)) {
                properties.load(fileInputStream);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            log.debug("meta-inf.properties file does not exist.");
        }

        if (properties.isEmpty()) {
            return;
        }

        out.print(", \"");
        JSHelper.escapeJS(out, properties.getProperty("serviceName"));
        out.print("\", \"");
        JSHelper.escapeJS(out, properties.getProperty("namespace"));
        out.print("\"");
    }

    private void deferCalls(String rootReference, List<Call> toDefer, List<String> tags, Map<String, ParameterInfoDto> paramInfo, BitSet requredIds){
        if(toDefer == null || toDefer.size() <= 0){
            return;
        }

        if(!deferredCalls.containsKey(rootReference)) {
            deferredCalls.put(rootReference, new DeferredCalls(toDefer, tags, paramInfo, requredIds));
            return;
        }

        DeferredCalls dc = deferredCalls.get(rootReference);
        dc.calls.addAll(toDefer);

        if(dc.tags.size() < tags.size()){
            dc.tags = tags;
        }

        dc.paramInfo.putAll(paramInfo);
        dc.requredIds.or(requredIds);
    }

    public void processCalls(String rootReference, ArrayList<Call> calls, List<String> tags, Map<String, ParameterInfoDto> paramInfo, BitSet requredIds) {
        List<Call> toDefer = new ArrayList<>(calls.size());
        List<Call> toPrint = new ArrayList<>(calls.size());
        for(Call call: calls){
            if(call.reactorChainId != null){
                toDefer.add(call);
            } else {
                toPrint.add(call);
            }
        }
        deferCalls(rootReference, toDefer, tags, paramInfo, requredIds);
        printCalls(rootReference, toPrint, tags, paramInfo, requredIds);
    }

    @Override
    public void postProcess(String rootReference) {
        DeferredCalls dc = deferredCalls.get(rootReference);
        if(dc != null) {
            List<Call> combinedCalls = combineReactorCalls(dc.calls);
            printCalls(rootReference, combinedCalls, dc.tags, dc.paramInfo, dc.requredIds);
        }
    }

    private boolean acceptCall(Call call) {
        if(cf == null) {
            return true;
        }
        return cf.filter(call);
    }

    private List<Call> combineReactorCalls(List<Call> toGroup){
        //process the calls that have been read in any case
        Map<String, List<Call>> collect = new HashMap<>();
        for(Call call: toGroup) {
            if(call.reactorChainId == null) {
                continue;
            }
            List<Call> byChainId = collect.get(call.reactorChainId);
            if(byChainId == null) {
                byChainId = new ArrayList<>();
                collect.put(call.reactorChainId, byChainId);
            }
            byChainId.add(call);
        }

        List<Call> result = new ArrayList<>(toGroup.size());

        for (Map.Entry<String, List<Call>> entry : collect.entrySet()) {
            Call newCall = new Call();
            newCall.params = new HashMap<>();
            newCall.time = Long.MAX_VALUE;
            newCall.reactorChainId = entry.getKey();
            long latestFinish = Long.MIN_VALUE;
            Set<String> affectedThreads = new HashSet<>();
            Set<String> callsStreamIndexes = new HashSet<>();
            for (Call call : entry.getValue()) {
                latestFinish = Math.max(latestFinish, call.time + call.duration);

                affectedThreads.add((call.threadName));
                if(!StringUtils.isBlank(call.callsStreamIndex)) {
                    callsStreamIndexes.add(call.callsStreamIndex);
                }
                newCall.time = Math.min(call.time, newCall.time);
                newCall.memoryUsed += call.memoryUsed;
                newCall.waitTime += call.waitTime;
                newCall.cpuTime += call.cpuTime;
                newCall.nonBlocking += call.nonBlocking;
                newCall.calls += call.calls;
                newCall.method = call.method;
                newCall.transactions += call.transactions;
                newCall.traceFileIndex = call.traceFileIndex;
                newCall.bufferOffset = call.bufferOffset;
                newCall.recordIndex = call.recordIndex;
                newCall.suspendDuration += call.suspendDuration;
                newCall.netRead += call.netRead;
                newCall.netWritten += call.netWritten;

                combineParams(call, newCall);
            }
            newCall.threadName = StringUtils.join(affectedThreads, "_");
            newCall.callsStreamIndex = StringUtils.join(callsStreamIndexes, "_");
            newCall.duration = (int) (latestFinish - newCall.time);
            result.add(newCall);
        }

        return result;
    }

    private void combineParams(Call call, Call newCall){
        if (call.params == null) {
            return;
        }
        for (Map.Entry<Integer, List<String>> integerListEntry : call.params.entrySet()) {
            Integer key = integerListEntry.getKey();
            List<String> srcList = integerListEntry.getValue();
            if(srcList == null || srcList.size() == 0){
                return;
            }

            List<String> theList = newCall.params.get(key);
            if(theList == null) {
                theList = new ArrayList<>();
                newCall.params.put(key, theList);
            }
            theList.addAll(srcList);
        }
    }

    public void printCalls(String rootReference, List<Call> calls, List<String> tags, Map<String, ParameterInfoDto> paramInfo, BitSet requredIds) {
        try { // root == profiler/dump/server_name/2010/06/10/123123123
            if (calls.isEmpty()) return;

            final boolean sameDir = rootReference.equals(prevDumpDir);
            if (!sameDir) {
                prevIds.clear();
                prevDumpDir = rootReference;
            }
            PrintWriter out = this.out;
            out.print("{ var f=CL.addFolder(\"");
            out.print(rootReference.replace('\\', '/'));
            out.print("\"");

            printAdditionalRootReferenceDetails(rootReference);

            out.println(");");
            out.println(" var t = f.tags;");
            int k = 0;
            for (int i = -1; (i = requredIds.nextSetBit(i + 1)) >= 0; ) {
                if (sameDir && prevIds.get(i)) continue;
                prevIds.set(i);
                String tag = i < tags.size() ? tags.get(i) : ("tag " + i);
                out.print("t.a(");
                out.print(i);
                out.print(",\"");
                JSHelper.escapeJS(out, tag);
                out.print("\");");
                k++;
                if (k == 10) {
                    out.println();
                    k = 0;
                }
            }

            if (!sameDir) {
                k = 0;
                for (ParameterInfoDto info : paramInfo.values()) {
                    out.print("t.b(\"");
                    JSHelper.escapeJS(out, info.name);
                    out.print("\",");
                    out.print(info.list ? '1' : '0');
                    out.print(',');
                    out.print(info.order);
                    out.print(',');
                    out.print(info.index ? '1' : '0');
                    out.print(",\"");
                    if (info.signatureFunction != null)
                        JSHelper.escapeJS(out, info.signatureFunction);
                    out.print("\");");
                    k++;
                    if (k == 10) {
                        out.println();
                        k = 0;
                    }
                }
            }

            out.println("var q=f.id;");
            out.println("var w=[];");
            Map<String, Integer> threadIdx = new HashMap<String, Integer>(20);
            for (Call call : calls) {
                String threadName = call.threadName;
                if (threadName == null)
                    continue;
                if (threadIdx.containsKey(threadName))
                    continue;
                int threadId = threadIdx.size();
                threadIdx.put(threadName, threadId);
                out.print("w[");
                out.print(threadId);
                out.print("]=\"");
                JSHelper.escapeJS(out, threadName);
                out.println("\";");
            }
            out.println("CL.append([");
            boolean commaRequired = false;
            for (Call call : calls) {
                if(!acceptCall(call)){
                    continue;
                }
                if (commaRequired)
                    out.print(',');
                else
                    commaRequired = true;
                printCall(call, threadIdx);
            }
            out.println("]);");
            out.println("}");
        } catch (Throwable t) {
            log.info("Unable to convert calls from {} to javascript", rootReference, t);
        }
    }

    private void printCall(Call call, Map<String, Integer> threadIdx) throws IOException {
        out.print("[");
        out.print(call.time - call.queueWaitDuration);
        out.print(',');
        out.print(call.duration + call.queueWaitDuration);
        out.print(',');
        out.print(call.nonBlocking);
        out.print(',');
        out.print(call.cpuTime);
        out.print(',');
        out.print(call.queueWaitDuration);
        out.print(',');
        out.print(call.suspendDuration);
        out.print(',');
        out.print(call.calls);
        out.print(",q,");

        String rowId;
        if (call.reactorChainId == null) {
            rowId = "q+'_" + call.traceFileIndex +
                    "_" + call.bufferOffset +
                    "_" + call.recordIndex +
                    "_" + call.reactorFileIndex +
                    "_" + call.reactorBufferOffset + "'";
        } else {
            rowId = "'chain_'+q+'_" + call.reactorChainId + "_" + call.callsStreamIndex + "'";
        }
        out.print(rowId);

        out.print(",");
        out.print(call.method);
        out.print(',');
        out.print(call.transactions);
        out.print(',');
        out.print(call.memoryUsed);
        out.print(',');
        out.print(call.logsGenerated);
        out.print(',');
        out.print(call.logsWritten);
        out.print(',');
        out.print(call.fileRead + call.fileWritten);
        out.print(',');
        out.print(call.fileWritten);
        out.print(',');
        out.print(call.netRead + call.netWritten);
        out.print(',');
        out.print(call.netWritten);
        if (call.params != null && !call.params.isEmpty()) {
            out.print(",{");
            boolean commaRequiredParams = false;
            if (call.threadName != null) {
                out.print("\"-5\":w["); // java.thread
                out.print(threadIdx.get(call.threadName));
                out.print(']');
                commaRequiredParams = true;
            }
            for (Map.Entry<Integer, List<String>> param : call.params.entrySet()) {
                if (!commaRequiredParams)
                    commaRequiredParams = true;
                else
                    out.print(',');
                final Integer id = param.getKey();
                if (id > 0)
                    out.print(id);
                else {
                    out.print('"');
                    out.print(id);
                    out.print('"');
                }
                List<String> value = param.getValue();
                int valueSize = value.size();
                out.print(':');
                if (valueSize > 1)
                    out.print('[');
                for (int i = 0; i < valueSize; i++) {
                    if (i != 0)
                        out.print(',');
                    out.print("\"");
                    JSHelper.escapeJS(out, value.get(i));
                    out.print("\"");
                }
                if (valueSize > 1)
                    out.print(']');
            }
            out.print('}');
        }
        out.println("]");
    }
}
