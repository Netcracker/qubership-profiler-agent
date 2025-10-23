package com.netcracker.cdt.ui.services;

import com.netcracker.cdt.ui.rest.v2.dto.Requests;
import com.netcracker.common.models.StreamType;
import com.netcracker.common.models.TimeRange;
import com.netcracker.common.models.pod.PodIdRestart;
import com.netcracker.common.models.pod.PodInfo;
import com.netcracker.common.models.pod.streams.StreamRegistry;
import com.netcracker.common.utils.DB;
import com.netcracker.persistence.PersistenceService;
import com.netcracker.persistence.adapters.cloud.cdt.dumps.CloudDumpsEntity;
import com.netcracker.persistence.adapters.cloud.cdt.dumps.CloudHeapDumpsEntity;
import io.quarkus.arc.lookup.LookupIfProperty;
import io.quarkus.logging.Log;
import io.quarkus.runtime.LaunchMode;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.StreamingOutput;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;

import java.io.InputStream;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

@LookupIfProperty(name = "service.type", stringValue = "ui")
@ApplicationScoped
public class CdtDumpsService {

    public static final DateTimeFormatter FILE_NAME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd'T'hhmmssVV").withZone(ZoneId.of("UTC"));

    @Inject
    PersistenceService persistence;
    @Inject
    LaunchMode launchMode;


    public record OutStream(String fileName, StreamingOutput httpStream) {
    }

    public List<PodInfo> findPods(String namespace, String service, TimeRange range) {
        var pods = persistence.pods.activePods(range);
        pods = pods.stream().filter(p -> {
            if (StringUtils.isNotBlank(namespace) && !namespace.equals(p.namespace())) return false;
            if (StringUtils.isNotBlank(service) && !service.equals(p.service())) return false;
            return true;
        }).toList();
        return pods;
    }

    public List<StreamRegistry> findRegistries(PodIdRestart pod, StreamType streamType, TimeRange range) {
        return persistence.streams.getRegistries(pod, streamType, range);
    }

    public OutStream prepareDownloadStream(String fileExt, List<StreamRegistry> registries) {
        String fileName;
        if (registries.size() == 1) {
            fileName = FILE_NAME_FORMATTER.format(registries.get(0).modifiedWhen());
        } else if (registries.isEmpty()) {
            fileName = "empty";
        } else {
            Instant earliest = registries.stream().map(StreamRegistry::modifiedWhen).min(Instant::compareTo).orElse(null);
            Instant latest = registries.stream().map(StreamRegistry::modifiedWhen).max(Instant::compareTo).orElse(null);

            fileName = FILE_NAME_FORMATTER.format(earliest) + "-" +
                    FILE_NAME_FORMATTER.format(latest);
        }

        return getOutput(fileName, fileExt, registries);
    }

    OutStream getOutput(String fileName, String fileExt, List<StreamRegistry> registries) {
        StreamingOutput stream = outputStream -> {
            ZipOutputStream zout = new ZipOutputStream(outputStream);
            for (StreamRegistry sr : registries) {
                String entryName = sr.podRestart().podName() + "/" + FILE_NAME_FORMATTER.format(sr.modifiedWhen()) + "." + fileExt + ".zip";
                try (InputStream is = persistence.streams.getStream(sr)) {
                    ZipEntry ze = new ZipEntry(entryName);
                    zout.putNextEntry(ze);
                    IOUtils.copy(is, zout);
                }
            }
            zout.finish();
            zout.flush();
        };
        return new OutStream(fileName, stream);
    }

    public StreamingOutput prepareStream(List<PodIdRestart> pods, StreamType streamType, Instant from, Instant to) {
        var range = TimeRange.of(from, to);
        return outputStream -> {
            ZipOutputStream zout = new ZipOutputStream(outputStream);
            for (var pod : pods) {
                var registries = persistence.streams.getRegistries(pod, streamType, range);
                Log.infof("found %d registries for filter [%s,%s] : %s - %s",
                        registries.size(), pod.toString(), streamType.toString(), from, to);
                for (StreamRegistry sr : registries) {
                    String entryName = sr.getZipEntryName(streamType);
                    try (InputStream is = persistence.streams.getStream(sr)) {
                        ZipEntry ze = new ZipEntry(entryName);
                        zout.putNextEntry(ze);
                        IOUtils.copy(is, zout);
                    }
                }
            }
            zout.finish();
            zout.flush();
        };
    }

    public StreamingOutput prepareStream(PodIdRestart pod, StreamType streamType) {
        var registries = persistence.streams.getLatestRegistry(pod, streamType);
        if (registries.isEmpty()) {
            Log.errorf("[%s] could not find registries of %s", pod.podId(), streamType);
            return null;
        }
        Log.infof("[%s] found %d registries of %s", pod.podId(), registries.size(), streamType);
        StreamRegistry sr = registries.get(0);
        Log.infof("[%s] sending data for registry %s", pod.podId(), sr);
        return prepareStream(sr);
    }

    public StreamingOutput prepareStream(PodIdRestart pod, StreamType streamType, int seqId) {
        var sr = persistence.streams.getStreamRegistryById(pod, streamType, seqId);
        if (sr.isEmpty()) {
            Log.errorf("[%s] could not find registry %d of %s ", pod.podId(), seqId, streamType);
            return null;
        }
        Log.infof("[%s] sending data for registry %s", pod.podId(), sr.get());
        return prepareStream(sr.get());
    }

    StreamingOutput prepareStream(StreamRegistry sr) {
        return outputStream -> {
            try (InputStream is = persistence.streams.getStream(sr)) {
                IOUtils.copy(is, outputStream);
            }
        };
    }

    public boolean deleteStream(PodIdRestart pod, StreamType streamType, int seqId) {
        var sr = persistence.streams.getStreamRegistryById(pod, streamType, seqId);
        if (sr.isEmpty()) {
            Log.errorf("[%s] could not find registry %d of %s ", pod.podId(), seqId, streamType);
            return false;
        }
        Log.infof("[%s] sending data for registry %s", pod.podId(), sr.get());

        try {
            persistence.batch.execute(
                    persistence.streams.deleteStreamData(sr.get())
            );
            persistence.batch.execute(
                    persistence.streams.deleteStreamRegistry(sr.get())
            );

            persistence.batch.flushIndexes(); // necessary for OpenSearch
            return true;
        } catch (Exception e) {
            Log.infof("[%s] error during deleting stream data: %s", pod.podId(), sr.get());
            return false;
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // HEAP DUMPS
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @DB
    public Collection<CloudHeapDumpsEntity> getHeapDumps(TimeRange range, List<Requests.Service> services) {

        var heaps = persistence.dumps.getHeapDumps(range);
        Log.infof("found %d heap dumps", heaps.size());

        // Convert filter list to Set for fast lookup
        Set<Requests.Service> allowed = new HashSet<>(services);

        return heaps.stream()
                .filter(h -> allowed.contains(new Requests.Service(h.namespace(), h.serviceName())))
                .sorted(Comparator.comparing(CloudHeapDumpsEntity::creationTime, Comparator.reverseOrder()))
                .toList();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // THREAD AND TOP DUMPS
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @DB
    public Collection<CloudDumpsEntity> getDumpObjects(String namespace, String service, TimeRange range) {

        var objects = persistence.dumps.getDumpObjects(namespace, service, range);
        Log.infof("found %d dump objects", objects.size());
        return objects;
    }
}
