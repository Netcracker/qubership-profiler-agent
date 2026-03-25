package com.netcracker.cdt.ui.services.tree;

import com.netcracker.profiler.model.CallRowId;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpServerRequest;
import org.apache.commons.lang.StringUtils;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public record CallTreeRequest(
        int callbackId,
        Boolean isZip,
        Integer paramsTrimSize,
        Integer paramTrimSizeForUI,
        long clientUTC,

        String callback,
        String pageState,
        String businessCategories,
        String adjustDuration,

        Map<String, Object> args, List<CallRowId> callIds,
        long begin, long end
) {

    public interface DumperConstants {
        byte EVENT_EMPTY = -1;
        byte EVENT_ENTER_RECORD = 0;
        byte EVENT_EXIT_RECORD = 1;
        byte EVENT_TAG_RECORD = 2;
        byte EVENT_FINISH_RECORD = 3;
        byte COMMAND_ROTATE_LOG = 1;
        byte COMMAND_FLUSH_LOG = 2;
        byte COMMAND_EXIT = 3;

        int TAGS_ROOT = -1;
        int TAGS_HOTSPOTS = -2;
        int TAGS_PARAMETERS = -3;
        int TAGS_CALL_ACTIVE = -4;
    }

    public static CallTreeRequest from(Instant t, boolean isZip, HttpServerRequest req) {
        return from(t, isZip, req.params());
    }

    public static CallTreeRequest from(Instant t, boolean isZip, MultiMap params) {
        int defaultTrimSize = isZip ? 200000000 : 15000;
        int trimSize = Integer.getInteger("com.netcracker.profiler.Profiler.PARAMS_TRIM_SIZE", defaultTrimSize);
        int trimUISize = params.get("params-trim-size") != null ? Integer.valueOf(params.get("params-trim-size")) : 15000;

        int callbackId = params.get("id") == null ? 0 : Integer.valueOf(params.get("id"));
        String callback = params.get("callback") == null ? "treedata" : params.get("callback");
        long clientUTC = params.get("clientUTC") == null ? System.currentTimeMillis() : Long.valueOf(params.get("clientUTC"));
        // start & end MUST be provided in request! but just to be safe:
        long start = params.get("s") != null ? Long.valueOf(params.get("s")) : t.minus(10, ChronoUnit.MINUTES).toEpochMilli() ;
        long end = params.get("e") != null ? Long.valueOf(params.get("e")) : t.toEpochMilli() ;

        List<String> treeIds = params.getAll("i");
        if (treeIds == null) treeIds = params.getAll("i[]");

        if (treeIds == null) {
            throw new IllegalArgumentException("treeIds should not be null");
        }

        var args = prepareArgs(params);
        if (isZip) {
            args.put("ro", "1");
        }
        args.put("i", treeIds);
//        args.put("i", treeIds.toArray());

        var callIds = treeIds.stream().
                filter(s -> !StringUtils.startsWith(s, "chain_")).
                map(call -> CallRowId.parse(call, args)).
                filter(Objects::nonNull).
                toList();
        if (callIds.isEmpty()) {
            return null;
        }

        // form parameters (POST)
        var pageState = args.getOrDefault("pageState", ""); // download from UI
        var businessCategories = args.getOrDefault("businessCategories", "");
        var adjustDuration = args.getOrDefault("adjustDuration", "");

        return new CallTreeRequest(callbackId, isZip, trimSize, trimUISize, clientUTC,
                callback,
                pageState.toString(), businessCategories.toString(), adjustDuration.toString(),
                args,
                callIds,
                start, end
        );
    }

    static Map<String, Object> prepareArgs(MultiMap params) {
        Map<String, Object> args = new HashMap<String, Object>();
        for (var key: params.names()) {
            if (!key.startsWith("f["))
                continue;
            args.put(key, params.getAll(key));
        }
        for (var key: params.names()) {
            if (key.charAt(0) != 'z') continue;
            var value = params.getAll(key);
            args.put(key, value.get(0));
        }
        return args;
    }
}
