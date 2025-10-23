package com.netcracker.common.models.pod.stat;

import com.netcracker.common.models.IStreamType;
import com.netcracker.common.models.StreamType;

import java.util.HashMap;
import java.util.Map;

public record PodDataAccumulated(Map<IStreamType, BlobSize> map) {

        public Map<String, Long> forDb(boolean original) {
                var res = new HashMap<String, Long>();
                for (var v: map.entrySet()) {
                        res.put(v.getKey().getName(), v.getValue().val(original));
                }
                return res;
        }

        public boolean has(IStreamType type) {
                return map.containsKey(type);
        }

        public boolean hasType(String streamName) {
                var t = StreamType.byName(streamName);
                if (t != null) {
                        return has(t);
                }
                return false;
        }

        public boolean hasGC() {
                return map.containsKey(StreamType.GC);
        }

        public boolean hasTops() {
                return map.containsKey(StreamType.TOP);
        }
        public boolean hasTD() {
                return map.containsKey(StreamType.TD);
        }

        public long sum(boolean original) {
                long r = 0;
                for (var v: map.values()) {
                        if (v != null) {
                                r += v.val(original);
                        }
                }
                return r;
        }

        public long rotationSize() {
                long r = 0;
                for (var k: map.keySet()) {
                        if (!k.isRotationRequired()) {
                                continue;
                        }
                        var v = map.get(k);
                        if (v != null) {
                                r += v.val(false);
                        }
                }
                return r;
        }

        public void min(PodDataAccumulated o) {
                o.map.forEach((key, v) -> {
                        map.put(key, BlobSize.min(v, map.getOrDefault(key, BlobSize.MAX)));
                });
        }

        public void max(PodDataAccumulated o) {
                o.map.forEach((key, v) -> {
                        map.put(key, BlobSize.max(v, map.getOrDefault(key, BlobSize.MIN)));
                });
        }

        public PodDataAccumulated append(IStreamType stream, boolean original, long bytes) {
                var s = map.getOrDefault(stream, BlobSize.empty());
                s = s.append(original, bytes);
                this.map.put(stream, s);
                return this;
        }

        public void overrideSum(long l) { // TODO hack for go collector
                for (var e: map.entrySet()) {
                        if (e.getValue().original != 0) {
                                e.getValue().override(l);
                                return;
                        }
                }
        }


        public static PodDataAccumulated empty() {
                return new PodDataAccumulated(new HashMap<>());
        }

        public static PodDataAccumulated of(Map<String, Long> original, Map<String, Long> compressed) {
                var map = fromDb(original, compressed).map;
                for (var e: original.entrySet()) {
                        var stream = StreamType.byName(e.getKey());
                        if (stream != null && stream.isAppendableStat()) {
                                // should not append old statistics for metadata streams
                                map.put(stream, BlobSize.empty());
                        }
                }
                return new PodDataAccumulated(map);
        }

        public static PodDataAccumulated fromDb(Map<String, Long> original, Map<String, Long> compressed) { // from db to dto
                var map = new HashMap<IStreamType, BlobSize>();
                for (var e: original.entrySet()) {
                        var stream = StreamType.byName(e.getKey());
                        if (stream == null) {
                                continue;
                        }
                        Long compress = compressed.getOrDefault(e.getKey(), 0L);
                        var s = new BlobSize(e.getValue(), compress);
                        map.put(stream, s);
                }
                return new PodDataAccumulated(map);
        }

}











