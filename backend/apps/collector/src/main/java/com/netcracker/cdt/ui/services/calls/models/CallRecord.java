package com.netcracker.cdt.ui.services.calls.models;

import com.netcracker.common.models.meta.dict.CallParameters;
import com.netcracker.common.models.pod.PodIdRestart;

import java.util.*;

import static com.netcracker.common.Consts.*;

public record CallRecord(
        long time,
        int duration,
        long nonBlocking,
        long cpuTime,
        int queueWaitDuration,
        int suspendDuration,
        int calls,
        PodIdRestart pod, // calculated
        String traceRecordId, // calculated
        String method, // calculated
        // Literal thread, // calculated
        long transactions,
        long memoryUsed,
        int logsGenerated,
        int logsWritten,
        long fileRead,
        long fileWritten,
        long netRead,
        long netWritten,
        CallParameters params // calculated
) implements Comparable<CallRecord> { // uniq index by (time + duration + method)

    public Object getAtIndex(int index) {
        return switch (index) {
            case C_TIME -> actualTimestamp();
            case C_NON_BLOCKING -> nonBlocking;
            case C_DURATION -> actualDuration();
            case C_CPU_TIME -> cpuTime;
            case C_QUEUE_WAIT_TIME -> queueWaitDuration;
            case C_SUSPENSION -> suspendDuration;
            case C_CALLS -> calls;
            case C_FOLDER_ID -> pod.oldPodName(); // calculated
            case C_ROWID -> traceRecordId; // calculated // not sorted
            case C_METHOD -> method; // calculated // not sorted
            case C_TRANSACTIONS -> transactions;
            case C_MEMORY_ALLOCATED -> memoryUsed;
            case C_LOG_GENERATED -> logsGenerated;
            case C_LOG_WRITTEN -> logsWritten;
            case C_FILE_TOTAL -> diskBytes();
            case C_FILE_WRITTEN -> fileWritten;
            case C_NET_TOTAL -> netBytes();
            case C_NET_WRITTEN -> netWritten;
            default -> null;
        };
    }

    public long actualTimestamp() {
        return time - queueWaitDuration;
    }

    public int actualDuration() {
        return duration + queueWaitDuration;
    }

    public long diskBytes() {
        return fileRead + fileWritten;
    }

    public long netBytes() {
        return netRead + netWritten;
    }

    private static final Set<Integer> intColumns = Set.of(
            C_DURATION, C_QUEUE_WAIT_TIME, C_SUSPENSION, C_CALLS, C_FOLDER_ID,
            C_LOG_GENERATED, C_LOG_WRITTEN);

    private static final Set<Integer> longColumns = Set.of(
            C_TIME, C_NON_BLOCKING, C_CPU_TIME, C_TRANSACTIONS, C_MEMORY_ALLOCATED,
            C_FILE_TOTAL, C_FILE_WRITTEN, C_NET_TOTAL, C_NET_WRITTEN);

    public static boolean isInt(int sortIndex) {
        return intColumns.contains(sortIndex);
    }

    public static boolean isLong(int sortIndex) {
        return longColumns.contains(sortIndex);
    }

    public static boolean isString(int sortIndex) {
        return sortIndex == C_FOLDER_ID;
    }

    public static boolean isComparable(int sortIndex) {
        return isInt(sortIndex) || isLong(sortIndex);
    }

    public static Comparator<CallRecord> comparator(int sortIndex, boolean asc) {
        final int k = coeff(asc);
        if (isInt(sortIndex)) {
            return Comparator.
                    <CallRecord>comparingInt(cr -> k * (Integer) cr.getAtIndex(sortIndex)).
                    thenComparing(cr -> cr);
        } else if (isLong(sortIndex)) {
            return Comparator.
                    <CallRecord>comparingLong(cr -> k * (Long) cr.getAtIndex(sortIndex)).
                    thenComparing(cr -> cr);
        } else if (isString(sortIndex)) {
            return Comparator.comparing(cr -> cr.getAtIndex(sortIndex).toString());
        } else {
            return null;
        }
    }

    private static int coeff(boolean asc) { // convert asc/desc to sign numeral coefficient
        if (asc)
            return 1;
        return -1;
    }


    @Override
    public int compareTo(CallRecord o) {
        var r = (int) (time - o.time);
        if (r == 0 && method != null) {
            r = method.compareTo(o.method);
        }
        if (r == 0) {
            r = duration - o.duration;
        }
        if (r == 0 && traceRecordId != null) {
            r = traceRecordId.compareTo(o.traceRecordId);
        }
        return r;
    }


    public boolean isIdleMethod() {
        return isIdleMethod(method);
    }

    public static boolean isIdleMethod(String name) {
        for (var knownIdleMethod : KNOWN_IDLE_METHODS) {
            if (name.contains(knownIdleMethod)) {
                return true;
            }
        }
        return false;
    }

}

