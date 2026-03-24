package com.netcracker.common.models.pod.streams;

import com.netcracker.cdt.collector.common.models.StreamInfoRequest;
import com.netcracker.common.models.StreamType;
import com.netcracker.common.models.pod.PodIdRestart;
import com.netcracker.common.models.pod.stat.BlobSize;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import static java.time.temporal.ChronoUnit.DAYS;

public record StreamRegistry(
        PodIdRestart podRestart,
        StreamType stream,
        int rollingSequenceId,
        Instant createdWhen,
        Instant modifiedWhen,
        BlobSize dataSize,    // when collector close stream in normal mode (by rolling period), it updates total bytes counter
                              // (createdWhen < rollingPeriod and total=0) is indication that connection was aborted
        Status status
) {

    public PodSequence asPodSequence() {
        return new PodSequence(podRestart, rollingSequenceId(), createdWhen(), modifiedWhen());
    }

    public String getPrimaryKey() {
        return String.format("%s-%s-%d", podRestart.oldPodName(), stream.getName(), rollingSequenceId);
    }

    public Instant day() {
        return createdWhen.truncatedTo(DAYS);
    }

    public boolean isMetaStream() {
        return stream().isMetaStream();
    }

    public String screenName() {
        return String.format("%s|%s[%d]", podRestart.podId(), stream, rollingSequenceId);
    }

    public void received(int contentLength) {
        dataSize.append(true, contentLength);
    }

    public void persisted(int contentLength) {
        dataSize.append(false, contentLength);
    }

    public String getZipEntryName(StreamType streamType) {
        return podRestart.podName() + "/" + FILE_NAME_FORMATTER.format(modifiedWhen()) + "." + streamType.getFileExtension() + ".zip";
    }

    public StreamRegistry close(Instant time) {
        return new StreamRegistry(podRestart, stream, rollingSequenceId, createdWhen, time, dataSize, Status.FINISHED);
    }

    public static StreamRegistry create(StreamInfoRequest req, int seqId) {
        return new StreamRegistry(req.id(), req.stream(), seqId, req.createdWhen(), req.modifiedWhen(), new BlobSize(0, 0), Status.CREATED);
    }

    public static final DateTimeFormatter FILE_NAME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd'T'hhmmssVV").withZone(ZoneId.of("UTC"));

    public enum Status {
        CREATED(0),
        FINISHED(9),
        INCORRECT(11);

        private final int val;

        Status(int i) {
            this.val = i;
        }

        public int getVal() {
            return val;
        }

        public static Status valueOf(int i) {
            return switch (i) {
                case 0 -> CREATED;
                case 9 -> FINISHED;
                default -> INCORRECT;
            };
        }
    }
}
