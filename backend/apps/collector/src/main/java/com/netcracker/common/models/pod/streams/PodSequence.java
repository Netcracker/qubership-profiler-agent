package com.netcracker.common.models.pod.streams;

import com.netcracker.common.models.StreamType;
import com.netcracker.common.models.pod.PodIdRestart;
import com.netcracker.common.models.pod.stat.BlobSize;

import java.time.Instant;
import java.util.Objects;

import static java.time.temporal.ChronoUnit.DAYS;

public record PodSequence(PodIdRestart pod, int sequenceId, Instant createdWhen, Instant modifiedWhen) implements Comparable<PodSequence> {

    public PodSequence expandCreatedModified(PodSequence other) {
        if (other == this
                || sequenceId != other.sequenceId
                || !pod.podName().equals(other.pod.podName())) {
            return this;
        }
        Instant created = createdWhen.isBefore(other.createdWhen) ? createdWhen : other.createdWhen;
        Instant modified = modifiedWhen.isAfter(other.modifiedWhen) ? modifiedWhen : other.modifiedWhen;
        return new PodSequence(pod, sequenceId, created, modified);
    }

    public StreamRegistry asStreamRegistry() {
        var st = StreamRegistry.Status.FINISHED;
        return new StreamRegistry(pod, StreamType.CALLS, sequenceId, createdWhen(), modifiedWhen(), BlobSize.empty(), st);
    }

    public Instant day() {
        return createdWhen.truncatedTo(DAYS);
    }

    public String podId() {
        return pod.podId();
    }

    public Instant restartTime() {
        return pod.restartTime();
    }

    @Override
    public int compareTo(PodSequence o) {
        var r = pod.podName().compareTo(o.pod.podName());
        if (r == 0) {
            r = sequenceId - o.sequenceId;
        }
        return r;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PodSequence)) return false;
        PodSequence that = (PodSequence) o;
        return pod.podName().equals(that.pod.podName()) && (sequenceId == that.sequenceId);
    }

    public boolean same(PodSequence o) { // comparison including ts
        if (this == o) return true;
        return pod.podName().equals(o.pod.podName()) && (sequenceId == o.sequenceId)
                && createdWhen.equals(o.createdWhen) && modifiedWhen.equals(o.modifiedWhen);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pod.podName(), sequenceId);
    }

    @Override
    public String toString() {
        return String.format("%s[seq:%s, %s - %s]", pod.oldPodName(), sequenceId, createdWhen, modifiedWhen);
    }
}
