package com.netcracker.common.models.meta;

import com.netcracker.common.models.Sizeable;
import com.netcracker.common.models.pod.PodIdRestart;

public record ParamsModel( // common database entity
        PodIdRestart pod,
        String paramName,
        boolean paramIndex,
        boolean paramList,
        int paramOrder,
        String signature
) implements Sizeable {

    public int getSize() {
        return pod.podId().length() +
                paramName.length() +
                signature.length() +
                // booleans occupy 5 bytes
                2 * BOOLEAN_MAX_LENGTH +
                // integers - 10 bytes,
                INTEGER_MAX_LENGTH +
                5 + TWO_BRACKETS_COMMA_AND_NEWLINE;
    }

}
