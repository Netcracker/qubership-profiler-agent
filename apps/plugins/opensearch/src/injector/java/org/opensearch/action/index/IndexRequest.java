package org.opensearch.action.index;

import org.opensearch.action.ActionRequest;
import org.opensearch.common.bytes.BytesReference;

public class IndexRequest extends ActionRequest {

    private String id;

    private String index;

    public native BytesReference source();

    public native String id();

    public String getIndex$profiler() {
        return index;
    }

}
