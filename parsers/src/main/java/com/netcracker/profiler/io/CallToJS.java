package com.netcracker.profiler.io;

import com.netcracker.profiler.configuration.ParameterInfoDto;

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

    public void processCalls(String rootReference, ArrayList<Call> calls, List<String> tags, Map<String, ParameterInfoDto> paramInfo, BitSet requredIds) {
        printCalls(rootReference, calls, tags, paramInfo, requredIds);
    }

    private boolean acceptCall(Call call) {
        if(cf == null) {
            return true;
        }
        return cf.filter(call);
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
        out.print(call.cpuTime);
        out.print(',');
        out.print(call.queueWaitDuration);
        out.print(',');
        out.print(call.suspendDuration);
        out.print(',');
        out.print(call.calls);
        out.print(",q,");

        String rowId = "q+'_" + call.traceFileIndex +
                    "_" + call.bufferOffset +
                    "_" + call.recordIndex + "'";
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
