package com.netcracker.profiler.io.xlsx;

import com.netcracker.profiler.configuration.ParameterInfoDto;
import com.netcracker.profiler.formatters.title.TitleFormatterFacade;
import com.netcracker.profiler.io.Call;
import com.netcracker.profiler.io.CallFilterer;
import com.netcracker.profiler.io.aggregate.Aggregator;
import com.netcracker.profiler.io.aggregate.model.AggregateRow;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.io.OutputStream;
import java.util.*;

@Component
@Scope("prototype")
public class AggregateCallsToXLSXListener implements ICallsToXLSXListener {
    protected CallToXLSX formatter;
    private CallFilterer cf;
    private Aggregator aggregator = new Aggregator();

    private Map<String, Object> formatContext;
    private boolean errorHappened;

    public AggregateCallsToXLSXListener(CallFilterer cf, OutputStream out, Map<String, Object> formatContext) {
        this.cf = cf;
        this.formatContext = formatContext;

        formatter = new CallToXLSX(out);
        formatter.nextRow();
        formatter.addText("Title");
        formatter.addText("Count");
        formatter.addText("Duration");
        formatter.addText("Duration(h)");
        formatter.addText("DurationPerExec");
        formatter.addText("Duration90thPercentile");
        formatter.addText("CpuTime");
        formatter.addText("CpuTime(h)");
        formatter.addText("CpuTimePerExec");
        formatter.addText("Queueing");
        formatter.addText("Suspension");
        formatter.addText("SuspensionPerExec");
        formatter.addText("Memory");
        formatter.addText("MemoryGb");
        formatter.addText("MemoryPerExec(Mb)");
    }

    @Override
    public void processCalls(String rootReference, ArrayList<Call> calls, List<String> tags, Map<String, ParameterInfoDto> paramInfo, BitSet requiredIds) {
        if (calls.isEmpty()) return;
        Map<String, Integer> tagToIdMap = buildTagToIdMap(tags);

        for(Call call: calls) {
            if (cf != null && !cf.filter(call)) {
                continue;
            }

            String title = TitleFormatterFacade.formatCommonTitle(tags.get(call.method), tagToIdMap, call.params, formatContext).getText();
            aggregator.processCall(call, title);
        }
    }

    private static Map<String, Integer> buildTagToIdMap(List<String> tags) {
        Map<String, Integer> tagToIdMap = new HashMap<>(tags.size());
        for(int i=0; i<tags.size(); i++) {
            tagToIdMap.put(tags.get(i), i);
        }
        return tagToIdMap;
    }

    public void postProcess() {
        if(!errorHappened) {
            Collection<AggregateRow> aggregateRows = aggregator.finish();
            for (AggregateRow aggregateRow : aggregateRows) {
                formatter.nextRow();
                formatter.addText(aggregateRow.getTitle());
                formatter.addNumber(aggregateRow.getCount());
                formatter.addNumber(aggregateRow.getDuration());
                formatter.addNumber(aggregateRow.getDurationHours());
                formatter.addNumber(aggregateRow.getDurationPerExec());
                formatter.addNumber(aggregateRow.getDuration90thPercentile());
                formatter.addNumber(aggregateRow.getCpuTime());
                formatter.addNumber(aggregateRow.getCpuTimeHours());
                formatter.addNumber(aggregateRow.getCpuTimePerExec());
                formatter.addNumber(aggregateRow.getQueueing());
                formatter.addNumber(aggregateRow.getSuspension());
                formatter.addNumber(aggregateRow.getSuspensionPerExec());
                formatter.addNumber(aggregateRow.getMemory());
                formatter.addNumber(aggregateRow.getMemoryGb());
                formatter.addNumber(aggregateRow.getMemoryPerExecMb());
            }
        }

        formatter.finish();
    }

    @Override
    public void processError(Throwable ex) {
        formatter.nextRow();
        formatter.addText(ex.toString());
        errorHappened = true;
    }
}
