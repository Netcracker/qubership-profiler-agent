package com.netcracker.cdt.ui.models;

import com.netcracker.common.models.pod.PodInfo;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public record PodsIndex(Map<String, PodMetaData> podInfos) {

    public PodMetaData ensure(PodInfo pod) {
        String name = pod.oldPodName();

        var already = podInfos.get(name);
        if (already != null) {
            return already;
        }

        PodMetaData result = PodMetaData.empty(pod);
        podInfos.put(name, result);
        return result;
    }

    public static PodsIndex create() {
        return new PodsIndex(new ConcurrentHashMap<>());
    }

    public int size() {
        return podInfos.size();
    }
}
