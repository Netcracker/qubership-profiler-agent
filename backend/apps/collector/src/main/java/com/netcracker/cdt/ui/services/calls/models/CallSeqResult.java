package com.netcracker.cdt.ui.services.calls.models;

import com.netcracker.common.models.pod.streams.PodSequence;

import java.util.*;
import java.util.stream.Stream;

public record CallSeqResult(PodSequence subTask, Type res, Stream<CallRecord> calls, int parsedCalls, List<Throwable> exceptions) {

    public boolean isSuccess() {
        return res == Type.SUCCESS;
    }

    public static CallSeqResult empty() {
        return new CallSeqResult(null, Type.SUCCESS, Stream.empty(), 0, List.of());
    }

    public static CallSeqResult success(PodSequence subTask, int parsedCalls, Stream<CallRecord> calls) {
        return new CallSeqResult(subTask, Type.SUCCESS, calls, parsedCalls, List.of());
    }

    public static CallSeqResult timeout(PodSequence subTask, int parsedCalls) {
        return new CallSeqResult(subTask, Type.TIMEOUT, Stream.empty(), parsedCalls, List.of());
    }

    public static CallSeqResult failed(PodSequence subTask, int parsedCalls, Exception e) {
        return new CallSeqResult(subTask, Type.FAILED, Stream.empty(), parsedCalls, List.of(e));
    }

    public enum Type{
        SUCCESS, TIMEOUT, FAILED
    }

    @Override
    public String toString() {
        return "{%s => %s,%d calls,%d err}".formatted(subTask.toString(), res, parsedCalls, exceptions.size());
    }
}