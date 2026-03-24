package com.netcracker.cdt.ui.services.tree.context;

import com.netcracker.common.models.meta.ClobIndex;
import com.netcracker.common.models.meta.Value;
import com.netcracker.common.models.StreamType;
import com.netcracker.common.models.meta.DictionaryIndex;
import com.netcracker.common.models.SuspendRange;
import com.netcracker.common.models.TimeRange;
import com.netcracker.common.models.pod.PodIdRestart;
import com.netcracker.persistence.PersistenceService;
import com.netcracker.profiler.sax.io.DataInputStreamEx;
import io.quarkus.arc.lookup.LookupIfProperty;
import io.quarkus.logging.Log;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.io.IOException;
import java.util.*;

@LookupIfProperty(name = "service.type", stringValue = "ui")
@Singleton
public class TreeDataLoader {

    @Inject
    PersistenceService dbPersistence;

    public DataInputStreamEx openDataInputStream(String podReference, StreamType streamType, int rollingSequenceId) throws IOException {
        var podId = PodIdRestart.of(podReference);
        var registry = dbPersistence.streams.getStreamRegistryById(podId, streamType, rollingSequenceId); // TODO not use
        if (registry.isEmpty()) {
            throw new IOException("Unknown stream " + streamType + ":" + rollingSequenceId + " for " + podReference);
        }
        var stream = dbPersistence.streams.getStream(registry.get()); // TODO use helper
        if (stream == null) {
            throw new IOException("Could not load stream " + streamType + ":" + rollingSequenceId + " for " + podReference);
        }
        var is = new DataInputStreamEx(stream);
        return is;
    }

    public DataInputStreamEx openTraceStream(String podReference, int fileIndex) throws IOException {
        // old-fashioned file indexes start from 1. Cassandra data model starts from zero
        return openDataInputStream(podReference, StreamType.TRACE, fileIndex - 1);
    }

    public DataInputStreamEx openClobStream(Value.ClobId id) throws IOException {
        // old-fashioned file indexes start from 1. Cassandra data model starts from zero
        return openDataInputStream(id.podReference(), id.clobType(), id.fileIndex() - 1);
    }

    public void loadSuspendLog(SuspendRange range, String podReference, long start, long end) {
        var podId = PodIdRestart.of(podReference);
        var found = dbPersistence.meta.getSuspends(podId, TimeRange.ofEpochMilli(start, end));
        range.addAll(found);
    }

    public void loadClobs(ClobIndex idx) {
        var arr = idx.uniqToLoad();
        try {
            for (var clob : arr) {
                var is = openClobStream(clob.id());
                idx.load(clob, is);
            }
        } catch (Exception t) {
            Log.errorf(t, "Unable to read %d clobs", arr.size());
        }
    }

    public void loadMeta(DictionaryIndex idx, String podReference, BitSet tagIds) {
        var podId = PodIdRestart.of(podReference);

        var params = dbPersistence.meta.getParams(podId);
        for (var param : params) {
            idx.putParameter(param);
        }

        var reqIds = dictIds(tagIds);
        var dictionary = dbPersistence.meta.getDictionary(podId, reqIds);
//        if (requiredIds.cardinality() != dictionary.size()) {
//            Log.warnf("Incorrect amount of tags loaded from DB. POD %s, tags required: %s", podReference, request);
//            Log.debugf("Tags loaded: %s", dict.stream().map(DictionaryTag::getPosition).toList());
//        }
        for (var tag : dictionary) {
            idx.putDictionary(tag.position(), tag.tag());
        }
    }

    List<Integer> dictIds(BitSet requiredIds) {
        if (requiredIds == null) return null;

        List<Integer> request = new ArrayList<>();
        for (int i = requiredIds.nextSetBit(0); i >= 0; i = requiredIds.nextSetBit(i + 1)) {
            request.add(i);
        }
        return request;
    }


}
