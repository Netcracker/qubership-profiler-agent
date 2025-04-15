package org.qubership.profiler.io.xlsx;

import org.qubership.profiler.configuration.ParameterInfoDto;
import org.qubership.profiler.io.Call;
import org.qubership.profiler.io.CallFilterer;

import org.springframework.context.annotation.Profile;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.io.OutputStream;
import java.net.URLEncoder;
import java.util.*;

@Component
@Scope("prototype")
@Profile("filestorage")
public class CallsToXLSXListener implements ICallsToXLSXListener {
    // Excel supports cell text up to 32767, so we will add '...' at the end to show that value is truncated
    private static final int MAX_CELL_TEXT_LENGTH = 32760;

    String serverAddress;
    protected CallToXLSX formatter;
    private CallFilterer cf;

    public CallsToXLSXListener(String serverAddress, CallFilterer cf, OutputStream out) {
        this.serverAddress = serverAddress;
        this.cf = cf;

        formatter = new CallToXLSX(out);
        formatter.nextRow();
        formatter.addText("Link");
        formatter.addText("Start timestamp");
        formatter.addText("Duration");
        formatter.addText("CPU Time(ms)");
        formatter.addText("Suspended(ms)");
        formatter.addText("Queue(ms)");
        formatter.addText("Calls");
        formatter.addText("Transactions");
        formatter.addText("Disk Read (B)");
        formatter.addText("Disk Written (B)");
        formatter.addText("RAM (B)");
        formatter.addText("Logs generated");
        formatter.addText("Logs written (B)");
        formatter.addText("Net read (B)");
        formatter.addText("Net written (B)");

        createCellCaptions();

        formatter.addText("POD");
        formatter.addText("method");
        formatter.addText("params");
    }

    protected void createCellCaptions() {
    }

    protected void createAdditionalCells(String rootReference) {
    }

    private String encodeURL(String value) {
        try {
            return URLEncoder.encode(value, "UTF-8");
        } catch (Exception e) {
            return value;
        }
    }

    @Override
    public void processCalls(String rootReference, ArrayList<Call> calls, List<String> tags, Map<String, ParameterInfoDto> paramInfo, BitSet requiredIds) {
        if (calls.isEmpty()) return;

        String serverName = rootReference;
        // Extract server name from rootReference (e.g. clust1/2022/08/03/1659478612505)
        if (rootReference.length() > 25 &&
            rootReference.substring(rootReference.length() - 25).matches("[/\\\\]\\d{4}[/\\\\]\\d{2}[/\\\\]\\d{2}[/\\\\]\\d{13}")) {
            serverName = rootReference.substring(0, rootReference.length() - 25);
        }

        StringBuilder paramsText = new StringBuilder(MAX_CELL_TEXT_LENGTH + 8);
        for(Call call: calls) {
            if (cf != null && !cf.filter(call)) {
                continue;
            }

            formatter.nextRow();
            formatter.addHyperlink(serverAddress + "/tree.html#params-trim-size=15000&f%5B_0%5D=" + encodeURL(rootReference.replace('\\', '/')) +
                                   "&i=" + "0_" + call.traceFileIndex + "_" + call.bufferOffset + "_" +
                                   call.recordIndex + "_" + call.reactorFileIndex + "_" + call.reactorBufferOffset
            );

            formatter.addDate(new Date(call.time - call.queueWaitDuration));
            formatter.addNumber(call.duration + call.queueWaitDuration);
            formatter.addNumber(call.cpuTime);
            formatter.addNumber(call.suspendDuration);
            formatter.addNumber(call.queueWaitDuration);
            formatter.addNumber(call.calls);
            formatter.addNumber(call.transactions);
            formatter.addNumber(call.fileRead);
            formatter.addNumber(call.fileWritten);
            formatter.addNumber(call.memoryUsed);
            formatter.addNumber(call.logsGenerated);
            formatter.addNumber(call.logsWritten);
            formatter.addNumber(call.netRead);
            formatter.addNumber(call.netWritten);

            createAdditionalCells(rootReference);

            formatter.addText(serverName);
            String title = tags.get(call.method);
            if(call.params == null) {
                formatter.addText(title);
                formatter.addEmpty();
            } else {
                paramsText.setLength(0);
                StringBuilder workBuffer = paramsText;
                boolean firstParameter = true;
                for(Map.Entry<Integer, List<String>> entry: call.params.entrySet()) {
                    String paramName = tags.get(entry.getKey());
                    if ("profiler.title".equals(paramName) && entry.getValue().size() == 1) {
                        // If params contain title, then print it instead of method name and remove from parameters
                        title = entry.getValue().get(0);
                    } else {
                        if (firstParameter) {
                            firstParameter = false;
                        } else {
                            workBuffer = append(workBuffer, "; ");
                        }

                        workBuffer = append(workBuffer, paramName);
                        workBuffer = append(workBuffer, "=");
                        boolean firstValue = true;
                        for(String value : entry.getValue()) {
                            if (firstValue) {
                                firstValue = false;
                            } else {
                                workBuffer = append(workBuffer, ",");
                            }
                            workBuffer = append(workBuffer, value);
                        }
                    }
                }
                formatter.addText(title);
                formatter.addText(paramsText.toString());
            }
        }
    }

    private StringBuilder append(StringBuilder sb, String text) {
        if (sb == null) {
            return null;
        }
        if (sb.length() + text.length() > MAX_CELL_TEXT_LENGTH) {
            sb.append(text, 0, MAX_CELL_TEXT_LENGTH - sb.length()).append(" ...");
            return null;
        }
        sb.append(text);
        return sb;
    }

    @Override
    public void postProcess(String rootReference) {

    }

    public void postProcess() {
        formatter.finish();
    }

    @Override
    public void processError(Throwable ex) {
        formatter.nextRow();
        formatter.addText(ex.toString());
    }
}
