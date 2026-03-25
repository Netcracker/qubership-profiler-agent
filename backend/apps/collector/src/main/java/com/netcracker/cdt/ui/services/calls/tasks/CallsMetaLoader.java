package com.netcracker.cdt.ui.services.calls.tasks;

import com.netcracker.cdt.ui.models.PodMetaData;
import com.netcracker.cdt.ui.models.PodsIndex;
import com.netcracker.cdt.ui.services.calls.CallsListRequest;
import com.netcracker.common.PersistenceType;
import com.netcracker.common.models.TimeRange;
import com.netcracker.common.models.pod.streams.PodSequence;
import com.netcracker.common.models.StreamType;
import com.netcracker.common.models.pod.streams.StreamRegistry;
import com.netcracker.common.utils.DB;
import com.netcracker.persistence.PersistenceService;
import com.netcracker.profiler.sax.io.DataInputStreamEx;
import io.quarkus.arc.lookup.LookupIfProperty;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.io.*;
import java.util.*;

/**
 * DB helper: encapsulate all required works with DB during calls' parsing
 */
@LookupIfProperty(name = "service.type", stringValue = "ui")
@ApplicationScoped
public class CallsMetaLoader {

    @ConfigProperty(name = "pod.ui.max.sequences.count", defaultValue="50") // UI_SEQUENCES_PER_POD
    int maxSequencesCount;

    @Inject
    PersistenceService persistence;

    @DB
    public ReloadTask createTask(CallsMetaLoader metaLoader, CallsListRequest newRequest, int concurrent) {
        return new CloudReloadTask(newRequest, persistence.cloud);
    }

    // Find all required pods by search query (were active in time range and fits for query)
    @DB
    public List<PodMetaData> findPods(PodsIndex pods, CallsListRequest r) {
        // retrieve list of actual pods
        var activePods = persistence.pods.activePods(r.timeRange());
        // filter according to query
        var foundPodList = r.filterPods(activePods);
        if (foundPodList.isEmpty()) {
            Log.infof("No pod found in range: %s", r.timeRange());
            return List.of();
        } else {
            Log.debugf("Found %d pods from %d active in %s", foundPodList.size(), activePods.size(), r.timeRange());
        }
        // prepare list of PodMetaData for found pods (use cache from PodsIndex if already loaded)
        var podList = foundPodList.stream().map(pods::ensure).toList();
        Log.infof("Found %d pods for data loading", podList.size());
        return podList;
    }

    // Load all parameters and dictionary for pod (see also `DICTIONARY_BATCH_LIMIT` hard limit)
    @DB
    PodMetaData findPodMetaData(PodMetaData pod) {
        var params = persistence.meta.getParams(pod.podId());
        var tags = persistence.meta.getDictionary(pod.podId());
        pod.enrichDb(params, tags);
        return pod;
    }

    // Find archives (sequences) in time range for pod, limited by `maxSequencesCount` config
    @DB
    List<PodSequence> callSequencesList(PodMetaData pod, TimeRange period) {
        var registries = persistence.streams.
                getRegistries(pod.podId(), StreamType.CALLS, period);
        var converted = registries.
                stream().map(StreamRegistry::asPodSequence).
                toList();
        List<PodSequence> list = new ArrayList<>(converted);
        Collections.reverse(list); // start from the latest one
        if (list.size() > maxSequencesCount) {
            Log.warnf("[%s] got %d calls archives in %s time range, limit to %d sequences",
                    pod.podId(), list.size(), period, maxSequencesCount);
            list = list.stream().limit(maxSequencesCount).toList();
        }
        return list;
    }

    // Download binary data of the sequence (by pod and seqId)
    @DB
    DataInputStreamEx getPodSequenceStream(PodSequence seq) {
        InputStream mergedStream = persistence.streams.getStream(seq.asStreamRegistry());
        if (mergedStream == null) {
            // in case stream has not been flushed yet when pod it has just been opened
            Log.infof("Failed to load empty stream calls for %s", seq.toString());
            return null;
        }
        return new DataInputStreamEx(mergedStream);
    }

}
