package com.netcracker.common.models.meta;

import com.netcracker.common.models.Sizeable;
import com.netcracker.common.models.pod.PodIdRestart;

public record DictionaryModel(
    PodIdRestart pod,
    int position,
    String tag
) implements Sizeable {

    public int getSize() {
        return pod.podId().length() +
                tag.length() +
                INTEGER_MAX_LENGTH +
                2 + TWO_BRACKETS_COMMA_AND_NEWLINE;
    }

}
