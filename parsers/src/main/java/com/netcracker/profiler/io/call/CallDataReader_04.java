package com.netcracker.profiler.io.call;

import com.netcracker.profiler.dump.DataInputStreamEx;
import com.netcracker.profiler.formatters.title.ProfilerTitle;
import com.netcracker.profiler.formatters.title.TitleFormatterFacade;
import com.netcracker.profiler.io.Call;
import com.netcracker.profiler.tags.Dictionary;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CallDataReader_04 extends CallDataReader_03 {
    public void read(Call dst, DataInputStreamEx calls, BitSet requiredIds) throws IOException {
        super.read(dst, calls, requiredIds);
        dst.transactions = calls.readVarInt();
        dst.queueWaitDuration = calls.readVarInt();
    }

    @Override
    public void postCompute(ArrayList<Call> result, Dictionary tags, BitSet requiredIds) {
        Map<String, Integer> tagToIdMap = buildTagToIdMap(tags, requiredIds);
        Integer titleTagId = tagToIdMap.get("profiler.title");
        if(titleTagId == null) {
            titleTagId = findTag(tags, "profiler.title");
        }
        if(titleTagId == null) {
            return;
        }
        requiredIds.set(titleTagId);

        for (Call call : result) {
            if (call.params != null) {
                String fullClassMethod = tags.get(call.method);
                String title = getSingleParameter(call, titleTagId);
                if(title == null) {
                    ProfilerTitle profilerTitle = TitleFormatterFacade.formatTitle(fullClassMethod, tagToIdMap, call.params);
                    if(profilerTitle != null && !profilerTitle.isDefault()) {
                        title = profilerTitle.getHtml();
                        if(title != null) {
                            setSingleParameter(call, titleTagId, title);
                        }
                    }
                }
            }
        }
    }

    private Map<String, Integer> buildTagToIdMap(Dictionary tags, BitSet requiredIds) {
        Map<String, Integer> tagToIdMap = new HashMap<>(requiredIds.cardinality());
        for (int i = requiredIds.nextSetBit(0); i >= 0; i = requiredIds.nextSetBit(i + 1)) {
            tagToIdMap.put(tags.get(i), i);
        }
        return tagToIdMap;
    }

    private Integer findTag(Dictionary tags, String tagName) {
        for(int i=0; i<tags.size(); i++) {
            if(tagName.equals(tags.get(i))) {
                return i;
            }
        }
        return null;
    }

    private String getSingleParameter(Call call, int tagId) {
        List<String> values = call.params.get(tagId);
        if(values == null || values.isEmpty()) {
            return null;
        }
        return values.get(0);
    };

    private void setSingleParameter(Call call, int tagId, String value) {
        call.params.put(tagId, Collections.singletonList(value));
    };
}
