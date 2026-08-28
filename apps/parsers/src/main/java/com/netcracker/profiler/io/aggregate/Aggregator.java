package com.netcracker.profiler.io.aggregate;

import com.netcracker.profiler.io.Call;
import com.netcracker.profiler.io.aggregate.model.AggregateRow;

import java.util.*;

public class Aggregator {
    public static final long MAX_CALLS_FOR_AGGREGATE_TO_EXCEL;
    public static final long MAX_DISTINCT_CALLS_FOR_AGGREGATE_TO_EXCEL;

    static {
        long val = Long.getLong("com.netcracker.profiler.agent.Profiler.MAX_CALLS_FOR_AGGREGATE_TO_EXCEL", 10000000l);
        MAX_CALLS_FOR_AGGREGATE_TO_EXCEL = val == -1 ? Long.MAX_VALUE : val;
        val = Long.getLong("com.netcracker.profiler.agent.Profiler.MAX_DISTINCT_CALLS_FOR_AGGREGATE_TO_EXCEL", 100000l);
        MAX_DISTINCT_CALLS_FOR_AGGREGATE_TO_EXCEL = val == -1 ? Long.MAX_VALUE : val;
    }
    private Map<String, AggregateRow> rows = new HashMap<>();

    private long callsCount;

    public void processCall(Call call, String title) {
        if(callsCount >= MAX_CALLS_FOR_AGGREGATE_TO_EXCEL) {
            return;
        }
        callsCount++;

        AggregateRow row = rows.get(title);
        if (row == null) {
            if(rows.size() >= MAX_DISTINCT_CALLS_FOR_AGGREGATE_TO_EXCEL) {
                return;
            }
            row = new AggregateRow();
            row.setTitle(title);
            rows.put(title, row);
        }

        row.setCount(row.getCount() + 1);
        int currentDuration = call.duration + call.queueWaitDuration;
        row.setDuration(currentDuration + row.getDuration());
        row.getAllDurations().add(currentDuration);
        row.setCpuTime(call.cpuTime + row.getCpuTime());
        row.setQueueing(call.queueWaitDuration + row.getQueueing());
        row.setSuspension(call.suspendDuration + row.getSuspension());
        row.setMemory(call.memoryUsed + row.getMemory());
    }

    public Collection<AggregateRow> finish() {
        for(AggregateRow row : rows.values()) {
            long duration = row.getDuration();
            row.setDurationHours(((double)duration)/1000/60/60);
            row.setDurationPerExec((double)duration/(double)row.getCount());
            row.setDuration90thPercentile(percentile(row.getAllDurations(), 90));

            long cpuTime = row.getCpuTime();
            row.setCpuTimeHours(((double)cpuTime)/1000/60/60);
            row.setCpuTimePerExec((double)cpuTime/(double)row.getCount());

            long suspension = row.getSuspension();
            row.setSuspensionPerExec((double)suspension/(double)row.getCount());

            long memory = row.getMemory();
            row.setMemoryGb(((double)memory)/1024/1024/1024);
            row.setMemoryPerExecMb(((double)memory/(double)row.getCount())/1024/1024);
        }
        if(callsCount >= MAX_CALLS_FOR_AGGREGATE_TO_EXCEL || rows.size() >= MAX_DISTINCT_CALLS_FOR_AGGREGATE_TO_EXCEL) {
            List<AggregateRow> result = new ArrayList<>(rows.values());
            AggregateRow row = new AggregateRow();
            if(callsCount >= MAX_CALLS_FOR_AGGREGATE_TO_EXCEL) {
                row.setTitle("MAX_CALLS_FOR_AGGREGATE_TO_EXCEL limit reached. Result is truncated. Narrow your selection or increase -Dcom.netcracker.profiler.agent.Profiler.MAX_CALLS_FOR_AGGREGATE_TO_EXCEL JVM arg.");
            } else {
                row.setTitle("MAX_DISTINCT_CALLS_FOR_AGGREGATE_TO_EXCEL limit reached. Result is truncated. Adjust urlReplacePatterns or increase -Dcom.netcracker.profiler.agent.Profiler.MAX_DISTINCT_CALLS_FOR_AGGREGATE_TO_EXCEL JVM arg.");
            }
            result.add(0, row);
            return result;
        }
        return rows.values();
    }

    private int percentile(List<Integer> durations, double percentile) {
        Collections.sort(durations);
        int index = (int) Math.ceil((percentile / 100.0) * durations.size());
        return durations.get(index-1);
    }

}
