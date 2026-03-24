package com.netcracker.cdt.ui.rest.v2.dto;

import com.netcracker.cdt.ui.models.PodMetaData;
import com.netcracker.cdt.ui.services.calls.CallsListResult;
import com.netcracker.cdt.ui.services.calls.models.CallRecord;
import com.netcracker.common.Time;
import com.netcracker.common.models.StreamType;
import com.netcracker.common.models.pod.Namespace;
import com.netcracker.common.models.pod.PodIdRestart;
import com.netcracker.common.models.pod.PodInfo;
import com.netcracker.common.models.pod.stat.HeapDump;
import com.netcracker.common.models.pod.stat.PodReport;
import com.netcracker.persistence.adapters.cloud.cdt.dumps.CloudHeapDumpsEntity;

import java.time.Instant;
import java.util.List;
import java.util.Map;

public class Responses {

    public record Container(String namespace, List<Service> services) {
        public static Container of(Instant now, Namespace n) {
            var services = n.services().values().stream().map(s -> {
                var max = s.getPods().stream().map(p -> p.lastActive().toEpochMilli()).max(Long::compareTo);
                var lastActive = max.orElseGet(now::toEpochMilli);
                return new Responses.Service(s.serviceName(), lastActive, s.getPods().size());
            }).toList();
            return new Responses.Container(n.getNamespace(), services);
        }
    }

    public record Service(String name, long lastAck, int activePods) {
    }

    public record Pod(String namespace, String service, String pod, long startTime, List<String> tags) {
        public static Pod of(PodInfo p, List<String> tags) {
            return new Pod(p.namespace(), p.service(), p.podName(), p.restartTime().toEpochMilli(), tags);
        }
    }

    public record ServiceDump(String namespace, String service, String pod, long startTime,
                              boolean onlineNow, long lastAck, String podId, List<String> tags,
                              long dataAvailableFrom, long dataAvailableTo,
                              List<DownloadOption> downloadOptions) {

        public static ServiceDump of(Time time, PodReport p) {
            var r = p.screenRange(time);
            long from = r.from().toEpochMilli();
            long to = r.to().toEpochMilli();
            var downloads = p.dumps().stream()
                    .map(d -> DownloadOption.of(p.oldPodName(), d, from, to))
                    .toList();
            return new ServiceDump(p.namespace(), p.service(), p.podName(),
                    p.restartTime().toEpochMilli(),
                    p.isOnlineNow(time),
                    p.lastActive().toEpochMilli(),
                    p.oldPodName(), p.getTagValues(),
                    from, to, downloads );
        }
    }

    public record DownloadOption(String typeName, String uri) {
        static DownloadOption of(String podName, StreamType dump, long from, long to) {
            var link = String.format("/cdt/v2/dumps/%s/%s/download?timeFrom=%d&timeTo=%d",
                    podName, dump.getName(), from, to);
            return new DownloadOption(dump.getName(), link);
        }
    }


    public record HeapDumpRecord(String namespace, String service, String pod, long startTime,
                                 String dumpId, long creationTime, long bytes
    ) {
        // Mapping Entity to DTO
        public static HeapDumpRecord of(CloudHeapDumpsEntity entity) {
            return new HeapDumpRecord(
                    entity.namespace(),
                    entity.serviceName(),
                    entity.podName(),
                    -1,
                    entity.handle(),
                    entity.creationTime().toEpochMilli(),
                    entity.fileSize()
            );
        }
        public static HeapDumpRecord of(HeapDump h) {
            var p = h.pod();
            return new HeapDumpRecord(p.namespace(), p.service(), p.podName(),
                    p.restartTime().toEpochMilli(),
                    h.seqId(),
                    h.createdAt().toEpochMilli(),
                    h.compressed() );
        }
    }


    public record CallsStatistic(Status status, List<Stat> calls) {
        public record Status(boolean finished, long found) {
        }
        public static CallsStatistic.Status of(CallsListResult.Status stat) {
            return new CallsStatistic.Status(stat.finished(), stat.filteredRecords());
        }

        public static List<CallsStatistic.Stat> convert(CallsListResult list) {
            return list.convertCalls(CallsStatistic.Stat::of);
        }
        public record Stat(long ts, int duration, int calls) {
            public static CallsStatistic.Stat of(CallRecord c) {
                return new Responses.CallsStatistic.Stat(c.actualTimestamp(), c.actualDuration(), c.calls());
            }
        }
    }

    public record CallsList(Status status, List<Row> calls) {

        public static List<Row> convert(CallsListResult list) {
            return list.convertCalls(Row::of);
        }

        public record Status(boolean finished, int progress,
                             String errorMessage,
                             long filteredRecords, long processedRecords) {
        }

        public static Status of(CallsListResult.Status stat) {
            return new Status(stat.finished(), 100, stat.error(), stat.filteredRecords(), stat.processedRecords());
        }

        public record Pod(String namespace, String service, String pod, long startTime) {
            public static Pod of(PodMetaData pod) {
                return new Pod(pod.namespace(), pod.service(),  pod.pod().podName(), pod.startTime().toEpochMilli());
            }
            public static Pod of(PodIdRestart pod) {
                return new Pod(pod.namespace(), pod.service(),  pod.pod().podName(), pod.restartTime().toEpochMilli());
            }
        }

        public record Row(long ts, int duration, long cpuTime, int suspend, int queue,
                          int calls, long transactions,
                          long diskBytes, long netBytes, long memoryUsed,
                          String title,
                          String traceId, Pod pod,
                          Map<String, List<String>> params) {

            public static Row of(CallRecord c) {
                return new Responses.CallsList.Row(c.actualTimestamp(),
                        c.actualDuration(), c.cpuTime(), c.suspendDuration(), c.queueWaitDuration(),
                        c.calls(), c.transactions(),
                        c.diskBytes(), c.netBytes(), c.memoryUsed(),
                        c.method(),
                        c.traceRecordId(),
                        Pod.of(c.pod()),
                        c.params().asMap()
                );
            }
        }
    }

}
