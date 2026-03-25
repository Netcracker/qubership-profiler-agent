package com.netcracker.profiler.sax.call;

import com.netcracker.profiler.sax.io.DataInputStreamEx;
import com.netcracker.profiler.model.Call;

import java.io.IOException;
import java.util.*;

public abstract class CallDataReaderBase implements CallDataReader {
    public void readParams(Call dst, DataInputStreamEx calls) throws IOException {
        if (dst.params != null && !dst.params.isEmpty()) {
            dst.params.clear();
        }
        int len = calls.readVarInt();
        if (len == 0) return;
        if (dst.params == null) {
            dst.params = new HashMap<>(len, 1.0f);
        }

        for (; len > 0; len--) {
            final int paramId = calls.readVarInt();
            int size = calls.readVarInt();
            if (size == 0)
                dst.params.put(paramId, Collections.<String>emptyList());
            else if (size == 1) {
                dst.params.put(paramId, Collections.singletonList(calls.readString()));
            } else {
                String[] result = new String[size];
                for (size--; size >= 0; size--)
                    result[size] = calls.readString();
                dst.params.put(paramId, Arrays.asList(result));
            }
        }
    }

    public void skipParams(Call dst, DataInputStreamEx calls) throws IOException {
        if (dst.params != null && !dst.params.isEmpty()) {
            dst.params.clear();
        }

        for (int len = calls.readVarInt(); len > 0; len--) {
            final int paramId = calls.readVarInt();
            int size = calls.readVarInt();
            for (; size > 0; size--)
                calls.skipString();
        }
    }
}
