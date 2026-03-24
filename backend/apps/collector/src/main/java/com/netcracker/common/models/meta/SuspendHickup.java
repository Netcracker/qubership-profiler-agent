package com.netcracker.common.models.meta;

import com.netcracker.common.models.Sizeable;
import com.netcracker.common.models.pod.PodIdRestart;

import java.time.Instant;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;

public record SuspendHickup(
        PodIdRestart pod,
        Instant time,
        int suspendTime
) implements Sizeable {

    public int getSize() {
        return pod.podId().length() +
                TIMESTAMP_MAX_LENGTH +
                INTEGER_MAX_LENGTH +
                2 + TWO_BRACKETS_COMMA_AND_NEWLINE;
    }

    public Instant truncatedToDays() {
        return time.truncatedTo(ChronoUnit.DAYS);
    }

    public Instant truncatedToMinutes() {
        return time.truncatedTo(ChronoUnit.MINUTES);
    }

    // TODO: rename it
    public int getSecMs() {
        return time.get(ChronoField.MILLI_OF_SECOND) % 60000;
    }
}
