package com.netcracker.persistence.adapters.cloud.cdt;

import com.netcracker.common.models.meta.DictionaryModel;
import com.netcracker.common.models.pod.PodIdRestart;

import java.time.Instant;

// PostgreSQL Table: dictionary
public record CloudDictionaryEntity(
        String podId,           // pod_id [text]
        String podName,         // pod_name [text], PK
        Instant restartTime,    // restart_time [timestamptz], PK
        Integer position,       // position [integer], PK
        String tag              // tag [text]
) {

    public static CloudDictionaryEntity prepare(DictionaryModel model) {
        var podId = model.pod().oldPodName();
        var podName = model.pod().podName();
        var restartTime = model.pod().restartTime();
        return new CloudDictionaryEntity(podId, podName, restartTime, model.position(), model.tag());
    }

    public DictionaryModel toModel(PodIdRestart pod) {
        return new DictionaryModel(pod, position, tag);
    }
}
