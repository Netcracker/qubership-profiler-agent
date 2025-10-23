package com.netcracker.persistence.adapters.cloud.cdt;

import com.netcracker.common.models.meta.ParamsModel;
import com.netcracker.common.models.pod.PodIdRestart;

import java.time.Instant;

// PostgreSQL Table: params
public record CloudParamsEntity(
        String podId,       // pod_id [text], PK
        String podName,     // pod_name [text], PK
        Instant restartTime, // restart_time [timestamptz], PK
        String paramName,   // param_name [text], PK
        Boolean paramIndex, // param_index [boolean]
        Boolean paramList,  // param_list [boolean]
        Integer paramOrder, // param_order [integer]
        String signature    // signature [text]
) {

    public static CloudParamsEntity prepare(ParamsModel model) {
        var podId = model.pod().oldPodName();
        var podName = model.pod().podName();
        var restartTime = model.pod().restartTime();
        return new CloudParamsEntity(podId, podName, restartTime, model.paramName(), model.paramIndex(), model.paramList(), model.paramOrder(), model.signature());
    }

    public ParamsModel toModel(PodIdRestart pod) {
        return new ParamsModel(pod, paramName, paramIndex, paramList, paramOrder, signature);
    }
}
