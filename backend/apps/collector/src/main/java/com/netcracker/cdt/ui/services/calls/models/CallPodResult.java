package com.netcracker.cdt.ui.services.calls.models;

import com.netcracker.cdt.ui.models.PodMetaData;

import java.util.List;

public record CallPodResult(PodMetaData task, Type res, int foundSequences, List<Throwable> exceptions) {

    public boolean isSuccess() {
        return res == Type.SUCCESS;
    }

    public static CallPodResult empty() {
        return new CallPodResult(null, Type.SUCCESS, 0, List.of());
    }

    public static CallPodResult success(PodMetaData task, int foundSequences) {
        return new CallPodResult(task, Type.SUCCESS, foundSequences, List.of());
    }

    public static CallPodResult timeout(PodMetaData task, int foundSequences) {
        return new CallPodResult(task, Type.TIMEOUT, foundSequences, List.of());
    }

    public static CallPodResult failed(PodMetaData task, int foundSequences, Exception e) {
        return new CallPodResult(task, Type.FAILED, foundSequences, List.of(e));
    }

    public enum Type{
        SUCCESS, TIMEOUT, FAILED
    }

}